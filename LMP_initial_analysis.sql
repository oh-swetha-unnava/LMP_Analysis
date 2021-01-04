drop table sunnava.lmp_analysis_tb1;
create table sunnava.lmp_analysis_tb1 as (
with wrtv_cases as (
select distinct ams_id,asset_id,cmh_id,source_system_id,device_type,
nvl(start_date,(select min(date) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY)) as start_date,close_date
from(
      select ams_id,asset_id,cmh_id,source_system_id,device_type,start_date,
      case when start_date = close_date then close_date
      when close_date is null then lead(close_date,1) over (partition by asset_id order by rn) end as close_date -- updated this statement on 12/31
      from ( select ams_id,asset_id,cmh_id,device_type,source_system_id,rn,
                    case when new_case = 1 then date end as start_date,
                    case when closed_case = 1 then date end as close_date
             from (SELECT A.*,
	                 CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 1
	                      WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY)
                              AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
	                        OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1
                        THEN 1 ELSE 0 END AS NEW_CASE,
	                 CASE WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL)
                        THEN 1
                        WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1
                        THEN 1
                        ELSE 0 END AS CLOSED_CASE,
                  row_number() over (partition by asset_id order by date) as rn
                  FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
                  --where ams_id = 68686
                  )
              )where  (start_date is not null or close_date is not null))
              where  (start_date is not null or close_date is not null)-- and ams_id = 68686
            )
            ,
clinic_info as (
      select distinct a.ams_id, a.source_system_id ,a.cmh_id,c.asset_tag,
             a.device_type,a.start_date,a.close_date,c.sku,device_apk_version as software_version,--c.source_system_id,
             DATEDIFF(day,start_date, nvl(close_date ,GETDATE()))+1 as case_duration,
             b.account_name,b.ranking as clinic_ranking, b.billing_street as street,b.billing_city as city,
             b.billing_state_province as state,g.client_id
          --  ,CASE WHEN C.ASSET_TAG = D.ASSET_TAG THEN 1 ELSE 0 END AS Retired_device,
          --  B.owner_id AS CSM_Account_Owner_ID, E.NAME AS CSM_Account_Owner
      from wrtv_cases a
      LEFT JOIN ams.assets c on a.ams_id = c.ams_id
      LEFT JOIN salesforce.accounts b on a.cmh_id = b.cmh_id
      --LEFT JOIN ams.wh_retired_assets_input D ON C.ASSET_TAG = D.ASSET_TAG
      left join mdm.devices g on g.asset_id = C.ASSET_TAG
      --LEFT JOIN customer_ops.users E ON B.owner_id = E.user_id
      --  LEFT JOIN customer_ops.unrealized_attrition_history F on a.ams_id = f.ams_id
      where upper(c.STATUS) = upper('Installed')
            )
,
min_date as(
		select min(date)  as min_dte
         FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY),

heartbeats as (
select distinct a.asset_tag,a.source_system_id, a.start_date,a.close_date, a.case_duration,
              5 - (case when count(distinct to_char(c.created_at,'yyyy-mm-dd')) = 0
            				      	    then count(distinct to_char(d.poll_last_utc,'yyyy-mm-dd'))
            				      	    else count(distinct to_char(c.created_at,'yyyy-mm-dd')) end) as days_with_no_heartbeat
      from clinic_info a
      left join (select *
                  from mdm.heartbeats
                  where created_at >= (select min_dte
                                       FROM min_date)) c
        on a.client_id = c.client_id
        and (c.created_at > a.start_date -6
             and c.created_at < a.start_date)
      left join (select *
                 from broadsign.monitor_polls_history
                 where cast(to_char(poll_last_utc,'yyyy-mm-dd') as date) >= (select min_dte
                                       										 FROM min_date)-6) d
                  on d.client_resource_id = a.source_system_id
                  and (cast(to_char(d.poll_last_utc,'yyyy-mm-dd') as date) > a.start_date-6
                       and cast(to_char(d.poll_last_utc,'yyyy-mm-dd') as date) < a.start_date)
                       group by 1,2,3,4,5)

select distinct a.* , b.days_with_no_heartbeat
from clinic_info a
left join heartbeats b ON A.ASSET_TAG = B.ASSET_TAG AND A.START_DATE = B.START_DATE);


----- what % age of devices had no heartbeats as well as no screens in every 5 day time frame

select device_type,count(*) as total_cases, sum(case when days_with_no_heartbeat > 0 then 1 else 0 end) as heartbeat_missing
from sunnava.lmp_analysis_tb1
group by 1
union
select 'ALL_CASES',count(*) as total_cases, sum(case when days_with_no_heartbeat > 0 then 1 else 0 end) as heartbeat_missing
from sunnava.lmp_analysis_tb1;
--AMP	       888	 17   1.913
--LMP	      5071	152   2.997
--ALL_CASES	5959	169   2.836

----- based on updated table 12/31
-- LMP	5585	161
-- AMP	1029	23
-- ALL_CASES	6614	184

select device_type,count(distinct asset_tag) as total_devices,
	   count(distinct case when days_with_no_heartbeat > 0 then asset_tag end) as heartbeat_missing
from sunnava.lmp_analysis_tb1
group by 1
union
select 'ALL_CASES',count(distinct asset_tag) as total_devices,
  count(distinct case when days_with_no_heartbeat > 0 then asset_tag end) as heartbeat_missing
from sunnava.lmp_analysis_tb1;
-- AMP	          856	   16    1.869
-- LMP	         1767	  120    6.791
-- ALL_devices	 2623	  136    5.184

----- based on updated table 12/31
-- AMP	970	19
-- LMP	1786	130
-- ALL_CASES	2756	149


----- what is the ratio of # of total cases and # of distinct devices

select case_cnt, count(distinct asset_tag) from (
select asset_tag, count(distinct start_date) as case_cnt from  sunnava.lmp_analysis_tb1
group by 1)
group by 1
order by 1;
/*
1	1106
2	395
3	611
4	410
5	94
6	6
7	1
*/
--- based on updated table 12/30
/*
1	1163
2	361
3	494
4	483
5	216
6	38
7	1
*/

select case_cnt, device_type, count(distinct asset_tag) from (
select asset_tag, device_type, count(distinct start_date) as case_cnt from  sunnava.lmp_analysis_tb1
group by 1,2)
group by 1,2
order by 1;


---- removing cases that are not closed when case count is 1
select a.case_cnt, a.device_type, count(distinct a.asset_tag) from (
select asset_tag, device_type, count(distinct start_date) as case_cnt from  sunnava.lmp_analysis_tb1
group by 1,2
having case_cnt = 1) a left join (select asset_tag, device_type, close_date from sunnava.lmp_analysis_tb1 ) b on a.asset_tag =b.asset_tag
where b.close_date is not null
group by 1,2
order by 1;
/*
1	LMP	232
1	AMP	91
*/
--- based on updated table 12/30
/*
1	LMP	216
1	AMP	164
*/

select device_type, count(distinct asset_tag) from sunnava.lmp_analysis_tb1
where asset_tag in (
select asset_tag from (
select asset_tag, device_type, count(distinct start_date) as case_cnt from  sunnava.lmp_analysis_tb1
group by 1,2
having count(distinct start_date) = 1) )
and close_date is not null
group by 1;
/*
LMP	232
AMP	91
*/
--- based on updated table 12/30

/*
LMP	216
AMP	164
*/

----- Cases Freq Distribution by software version

select software_version, device_type,count(distinct asset_tag) as dev_cnt,count(*) as case_cnt
from  sunnava.lmp_analysis_tb1
group by 1,2
order by 2,1  ;


---select distinct product_use from sunnava.lmp_analysis_v1;
--Waiting Room Screen

---- asset tags (media player SKU , TV screen SKU  or both media player and tv screen sku)
select sku, device_type,count(distinct asset_tag)as dev_cnt ,count(*) as case_cnt
from  sunnava.lmp_analysis_tb1
group by 1,2
order by 2,1 ;
/*
SELECT DISTINCT sku_code,name, COUNT(DISTINCT AMS_ID) AS TOTAL_WRS,
COUNT(DISTINCT(CASE WHEN E.NAME = 'LINUX_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_LMP,
COUNT(DISTINCT(CASE WHEN E.NAME = 'ANDROID_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_AMP
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
and ((A.EXPORT_DATE -1)::DATE <= cast('2020-12-17' as date)
and (A.EXPORT_DATE -1)::DATE <= cast('2020-10-08'as date))
and UPPER(A.ASSET_TAG) in (select distinct asset_tag from sunnava.lmp_analysis_tb1 )
group by 1,2
order by 2,1;

select sku_2, device_type,count(distinct asset_tag)as dev_cnt ,count(*) as case_cnt
from ( select distinct a.*, nvl(sku_code,sku) as sku_2
from  sunnava.lmp_analysis_tb1 a
inner join
(SELECT DISTINCT sku_code,ASSET_TAG
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
and ((A.EXPORT_DATE -1)::DATE <= cast('2020-12-17' as date)
and (A.EXPORT_DATE -1)::DATE <= cast('2020-10-08'as date))
and UPPER(A.ASSET_TAG) in (select distinct asset_tag from sunnava.lmp_analysis_tb1 ) ) b on a.asset_tag = b.asset_tag)
group by 1,2
order by 2,1 desc ;
*/
----- can we further break down the table into the screen skus, device sku and software versions  for each row of LMP -
----- group by in the order that has less groupings

select sku,software_version, count(distinct asset_tag)as dev_cnt ,count(*) as case_cnt
from  sunnava.lmp_analysis_tb1
where device_type = 'LMP'
group by 1,2
order by 1,2 ;

--- graph between clinic rank and # of new cases for LMP and AMP
select clinic_ranking,device_type, count(distinct asset_tag)as dev_cnt ,count(*) as case_cnt
from  sunnava.lmp_analysis_tb1
group by 1,2
order by 1 desc,2  ;

select min(start_date),max(start_date) from sunnava.lmp_analysis_tb1;
--2020-10-08	2020-12-17

------ Totals ------

select count(distinct asset_tag) from sunnava.lmp_analysis_tb1 ;
--2756

select device_type, count(distinct asset_tag) from sunnava.lmp_analysis_tb1
group by 1;
/*
LMP	1786
AMP	970
*/

SELECT DISTINCT COUNT(DISTINCT AMS_ID) AS TOTAL_WRS,
COUNT(DISTINCT(CASE WHEN E.NAME = 'LINUX_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_LMP,
COUNT(DISTINCT(CASE WHEN E.NAME = 'ANDROID_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_AMP
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
and ((A.EXPORT_DATE -1)::DATE <= cast('2020-12-30' as date)--cast('2020-12-17' as date)
and (A.EXPORT_DATE -1)::DATE <= cast('2020-10-08'as date));
--44962	36212	8642
--44962	36212	8642 -- 12/30

SELECT DISTINCT sku_code, COUNT(DISTINCT AMS_ID) AS TOTAL_WRS,
COUNT(DISTINCT(CASE WHEN E.NAME = 'LINUX_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_LMP,
COUNT(DISTINCT(CASE WHEN E.NAME = 'ANDROID_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_AMP
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
and ((A.EXPORT_DATE -1)::DATE <= cast('2020-12-30' as date)--cast('2020-12-17' as date)
and (A.EXPORT_DATE -1)::DATE <= cast('2020-10-08'as date))
group by 1
order by 1;


SELECT DISTINCT device_apk_version, COUNT(DISTINCT AMS_ID) AS TOTAL_WRS, COUNT(DISTINCT(CASE WHEN E.NAME = 'LINUX_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_LMP, COUNT(DISTINCT(CASE WHEN E.NAME = 'ANDROID_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_AMP
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
left join mdm.devices g on g.asset_id = a.ASSET_TAG
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
and ((A.EXPORT_DATE -1)::DATE <= cast('2020-12-30' as date)-- cast('2020-12-17' as date)
and (A.EXPORT_DATE -1)::DATE <= cast('2020-10-08'as date))
group by 1
order by 1;

SELECT DISTINCT sku_code, device_apk_version,COUNT(DISTINCT AMS_ID) AS TOTAL_WRS,
COUNT(DISTINCT(CASE WHEN E.NAME = 'LINUX_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_LMP,
COUNT(DISTINCT(CASE WHEN E.NAME = 'ANDROID_MEDIA_PLAYER' THEN A.AMS_ID ELSE NULL END)) AS TOTAL_AMP
FROM AMS.ASSETS_HISTORY A
LEFT JOIN SALESFORCE.ACCOUNTS B ON A.CMH_ID = B.CMH_ID
LEFT JOIN ASSET_STATUS_ENGINE.ASSET C ON UPPER(A.ASSET_TAG) = UPPER(C.FIELD_SERVICES_TAG)
LEFT JOIN ASSET_STATUS_ENGINE.SKU D ON C.SKU_ID = D.ID
LEFT JOIN ASSET_STATUS_ENGINE.SKU_TYPE E ON D.SKU_TYPE_ID = E.ID
left join mdm.devices g on g.asset_id = a.ASSET_TAG
WHERE PRODUCT = 'Waiting Room Screen'
AND A.STATUS = 'Installed'
AND ranking IS NOT NULL
and ((A.EXPORT_DATE -1)::DATE <= cast('2020-12-17' as date)
and (A.EXPORT_DATE -1)::DATE <= cast('2020-10-08'as date))
group by 1,2
order by 1,2;

------------ reboot analysis --------------
select min(x.reboot_time)
from (SELECT d.asset_id, d.client_id, d.status, d.last_seen_at, UPPER(mac_address),t1.reboot_time
FROM devices d
    JOIN activities as a on d.id = a.device_id
    left join (SELECT DISTINCT t.taggable_id as id,t.created_at as reboot_time
                   FROM taggings as t
                   WHERE t.tag_id IN (select t2.id from tags t2 where t2.name like 'lmp_reboot%')
                   and created_at >  '2020-08-31') as t1 on d.id = t1.id
WHERE d.type = 'LinuxMediaPlayer'
    and a.id in (SELECT max(id) FROM activities WHERE script_id = 112 and created_at >'2020-08-31' group by device_id)
    and a.status in ('acknowledged', 'success')
ORDER BY d.asset_id) x;

drop table sunnava.lmp_reboot_analysis_tb1;
create table sunnava.lmp_reboot_analysis_tb1 as
select distinct a.*,cast(b.reboot_time as date) as reboot_date,
case when cast(b.reboot_time as date) >= start_date and  cast(b.reboot_time as date) <= nvl(close_date,getdate())
then 1 else 0 end as reboot_flag,
DATEDIFF(DAY,cast(b.reboot_time as date) , start_date ) as days_btw_reboot_new_case
from sunnava.lmp_analysis_tb1 a
left join mdm.devices d on d.asset_id = a.ASSET_TAG
left join sunnava.lmp_reboot_1001_1222 b on d.client_id = b.client_id
where device_type = 'LMP';

select days_btw_reboot_new_case, count(distinct asset_tag),count(distinct asset_tag||start_date)
from (select *, case when reboots >1 and rn =1 and days_btw_reboot_new_case < 31 then 1
			   when reboots = 1 and rn = 1 then 1
			   else 0 end as flag -- creating a flag to ignore the rows with past date of the
      from (select *, row_number() over (partition by asset_tag,reboot_date order by start_date) as rn
            from(select a.asset_tag,start_date,close_date,reboot_date,reboot_flag ,days_btw_reboot_new_case ,b.reboots
                 from sunnava.lmp_reboot_analysis_tb1 a
                 left join (select asset_tag, count(distinct reboot_date)  as reboots
		                        from sunnava.lmp_reboot_analysis_tb1 group by 1 )b on a.asset_tag=b.asset_tag
--where a.asset_tag = 'AHP2UA8391DFN'--11355 --30114-- 13844
                )
            where days_btw_reboot_new_case > 0)
--where rn = 1 and
--days_btw_reboot_new_case > 30
      )
where flag = 1
group by 1
order by 1;

select reboots, count(distinct asset_tag) from (
select asset_tag, sum(reboot_flag) as reboots from  sunnava.lmp_reboot_analysis_tb1
group by 1)
group by 1
order by 1 desc;

select asset_id from sunnava.lmp_reboot_1001_1222 where reboot_time like '2020-11-12%'
intersect
select asset_id from sunnava.lmp_reboot_1001_1222 where reboot_time like '2020-11-11%';
-- 0 assets

select count(distinct asset_id)
from sunnava.lmp_reboot_1001_1222;
--1160

select count(distinct ASSET_TAG) from(
select ASSET_TAG from sunnava.lmp_analysis_tb1
intersect
select asset_id from sunnava.lmp_reboot_1001_1222 );
-- 1080

select reboot_time, count(distinct asset_id)
from sunnava.lmp_reboot_1001_1222
group by 1;
/*
2020-12-02 11:21:32	618
2020-11-12 21:34:45	753
2020-11-11 17:39:32	109
	3
*/

select reboot_date, count(distinct asset_tag)
from sunnava.lmp_reboot_analysis_tb1
where reboot_flag = 1
group by 1
order by 1;
--2020-11-11	103
--2020-11-12	702
--2020-12-02	476

select reboot_time, count(distinct asset_id)
from sunnava.lmp_reboot_1001_1222
where asset_id in (select distinct ASSET_TAG from sunnava.lmp_analysis_tb1)
group by 1
order by 1;
/*
2
2020-11-11 17:39:32	109
2020-11-12 21:34:45	749
2020-12-02 11:21:32	542
*/

-- in the given timeframe what % age of distinct devices didn't need a second reboot
select distinct asset_id
from sunnava.lmp_reboot_1001_1222
where reboot_time in ('2020-11-11 17:39:32','2020-11-12 21:34:45')
INTERSECT
select distinct asset_id
from sunnava.lmp_reboot_1001_1222
where reboot_time in ('2020-12-02 11:21:32');
--323 devices

-- % repeat offenders
select count(distinct asset_tag),count(distinct asset_tag||start_date)
from (select *, case when reboots >1 and rn =1 and days_btw_reboot_new_case < 31 then 1
			   when reboots = 1 and rn = 1 then 1
			   else 0 end as flag -- creating a flag to ignore the rows with past date of the
      from (select *, row_number() over (partition by asset_tag,reboot_date order by start_date) as rn
            from(select a.asset_tag,start_date,close_date,reboot_date,reboot_flag ,days_btw_reboot_new_case ,b.reboots
                 from sunnava.lmp_reboot_analysis_tb1 a
                 left join (select asset_tag, count(distinct reboot_date)  as reboots
		                        from sunnava.lmp_reboot_analysis_tb1 group by 1 )b on a.asset_tag=b.asset_tag
--where a.asset_tag = 'AHP2UA8391DFN'--11355 --30114-- 13844
                )
            where days_btw_reboot_new_case > 0)
--where rn = 1 and
--days_btw_reboot_new_case > 30
      )
where flag = 1;
--946 1244

-------- QC ------
select count(distinct asset_tag) from (
select asset_tag,min(start_date),max(start_date),min(reboot_date),max(reboot_date),
case when min(reboot_date) >= min(start_date) and min(reboot_date) <= max(start_date) then 1
     when max(reboot_date) >= min(start_date) and max(reboot_date) <= max(start_date) then 1
     else 0 end as flag
from  sunnava.lmp_reboot_analysis_tb1
group by asset_tag)
where flag = 1;
--946

-----------------------------------   VIEW ------------------------------------
create or replace view sunnava.lmp_analysis_v1 as (
with wrtv_cases as (
select distinct ams_id,asset_id,cmh_id,source_system_id,device_type,
nvl(start_date,(select min(date) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY)) as start_date,close_date
from(
      select ams_id,asset_id,cmh_id,source_system_id,device_type,start_date,
      lead(close_date,1) over (partition by asset_id order by rn) as close_date
      from ( select ams_id,asset_id,cmh_id,device_type,source_system_id,rn,
                    case when new_case = 1 then date end as start_date,
                    case when closed_case = 1 then date end as close_date
             from (SELECT A.*,
	                 CASE WHEN DATE = (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY) THEN 1
	                      WHEN (DATE != (SELECT MIN(DATE) FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY)
                              AND DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) IS NULL)
	                        OR DATEDIFF(DAY,DATE,LAG(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE)) != -1
                        THEN 1 ELSE 0 END AS NEW_CASE,
	                 CASE WHEN (DATE != TO_CHAR(GETDATE(), 'YYYY-MM-DD') AND LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE) IS NULL)
                        THEN 1
                        WHEN DATEDIFF(DAY,DATE,NVL(LEAD(DATE) OVER (PARTITION BY ASSET_ID ORDER BY DATE),GETDATE())) > 1
                        THEN 1
                        ELSE 0 END AS CLOSED_CASE,
                  row_number() over (partition by asset_id order by date) as rn
                  FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY  A
                  --where ams_id = 68686
                  )
              )where  (start_date is not null or close_date is not null))
              where  (start_date is not null or close_date is not null)-- and ams_id = 68686
            )
            ,
clinic_info as (
      select distinct a.ams_id, a.source_system_id ,a.cmh_id,c.asset_tag,
             a.device_type,a.start_date,a.close_date,c.sku,device_apk_version as software_version,--c.source_system_id,
             DATEDIFF(day,start_date, nvl(close_date ,GETDATE()))+1 as case_duration,
             b.account_name,b.ranking as clinic_ranking, b.billing_street as street,b.billing_city as city,
             b.billing_state_province as state,g.client_id
          --  ,CASE WHEN C.ASSET_TAG = D.ASSET_TAG THEN 1 ELSE 0 END AS Retired_device,
          --  B.owner_id AS CSM_Account_Owner_ID, E.NAME AS CSM_Account_Owner
      from wrtv_cases a
      LEFT JOIN ams.assets c on a.ams_id = c.ams_id
      LEFT JOIN salesforce.accounts b on a.cmh_id = b.cmh_id
      --LEFT JOIN ams.wh_retired_assets_input D ON C.ASSET_TAG = D.ASSET_TAG
      left join mdm.devices g on g.asset_id = C.ASSET_TAG
      --LEFT JOIN customer_ops.users E ON B.owner_id = E.user_id
      --  LEFT JOIN customer_ops.unrealized_attrition_history F on a.ams_id = f.ams_id
      where upper(c.STATUS) = upper('Installed')
            )
,
min_date as(
		select min(date)  as min_dte
         FROM CAMPAIGN_DELIVERY.DEVICES_PERSISTENT_SCREEN_ISSUE_DAILY),

heartbeats as (
select  distinct a.asset_tag,a.source_system_id, a.start_date,a.close_date, a.case_duration,
              5 - (case when count(distinct to_char(c.created_at,'yyyy-mm-dd')) = 0
            				      	    then count(distinct to_char(d.poll_last_utc,'yyyy-mm-dd'))
            				      	    else count(distinct to_char(c.created_at,'yyyy-mm-dd')) end) as days_with_no_heartbeat
      from clinic_info a
      left join (select *
                  from mdm.heartbeats
                  where created_at >= (select min_dte
                                       FROM min_date)) c
        on a.client_id = c.client_id
        and (c.created_at > a.start_date -6
             and c.created_at < a.start_date)
      left join (select *
                 from broadsign.monitor_polls_history
                 where cast(to_char(poll_last_utc,'yyyy-mm-dd') as date) >= (select min_dte
                                       										 FROM min_date)-6) d
                  on d.client_resource_id = a.source_system_id
                  and (cast(to_char(d.poll_last_utc,'yyyy-mm-dd') as date) > a.start_date-6
                       and cast(to_char(d.poll_last_utc,'yyyy-mm-dd') as date) < a.start_date)
                       group by 1,2,3,4,5)

select distinct a.* , b.days_with_no_heartbeat
from clinic_info a
left join heartbeats b ON A.ASSET_TAG = B.ASSET_TAG AND A.START_DATE = B.START_DATE
)
;

drop table if exists ids.succesful_con_sites;
create table ids.succesful_con_sites as
select distinct ids.site_name from nft_production.logs nl
join nft_production.sites ns on nl.site_name = ns.`name`
join ids.sites ids on ids.site_id = ns.site_id 
where date(nl.created_at) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) and nl.status like '%exit 0%';

drop table if exists rds.tx_date_range;
create table rds.tx_date_range as
select
	*
from
	(
	select
		adddate('1970-01-01', t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) selected_date
	from
		(
		select
			0 i
	union
		select
			1
	union
		select
			2
	union
		select
			3
	union
		select
			4
	union
		select
			5
	union
		select
			6
	union
		select
			7
	union
		select
			8
	union
		select
			9) t0,
		(
		select
			0 i
	union
		select
			1
	union
		select
			2
	union
		select
			3
	union
		select
			4
	union
		select
			5
	union
		select
			6
	union
		select
			7
	union
		select
			8
	union
		select
			9) t1,
		(
		select
			0 i
	union
		select
			1
	union
		select
			2
	union
		select
			3
	union
		select
			4
	union
		select
			5
	union
		select
			6
	union
		select
			7
	union
		select
			8
	union
		select
			9) t2,
		(
		select
			0 i
	union
		select
			1
	union
		select
			2
	union
		select
			3
	union
		select
			4
	union
		select
			5
	union
		select
			6
	union
		select
			7
	union
		select
			8
	union
		select
			9) t3,
		(
		select
			0 i
	union
		select
			1
	union
		select
			2
	union
		select
			3
	union
		select
			4
	union
		select
			5
	union
		select
			6
	union
		select
			7
	union
		select
			8
	union
		select
			9) t4) v
where
	selected_date between (
	select
		MAKEDATE(YEAR(curdate()), 1) + INTERVAL QUARTER(curdate()) QUARTER - INTERVAL 1 QUARTER) and date_sub(CURDATE(), interval 1 day)
order by
	1 desc
limit 30;

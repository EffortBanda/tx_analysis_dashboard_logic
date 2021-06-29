drop table if exists ids.succesful_con_sites;
create table ids.succesful_con_sites as
select distinct ids.site_name from nft_production.logs nl
join nft_production.sites ns on nl.site_name = ns.`name`
join ids.sites ids on ids.site_id = ns.site_id 
where date(nl.created_at) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) and nl.status like '%exit 0%';

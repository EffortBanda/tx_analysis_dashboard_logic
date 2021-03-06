delete from tx_overdue_appointments_all_report_dates toaard where toaard.facility_name not in (select site_name from ids.succesful_con_sites); 
drop table tx_appointments_test ;
create table tx_appointments_test as
select
	distinct loc.district,
	cast(loc.facility_name as CHAR(255)) facility_name,
	loc.prime_partner,
	date(a.appointment_date) appointment_date,
	e.person_id,
	case
		when p.gender = '0' then 'F'
		when p.gender = '1' then 'M'
		when p.gender is null then 'NA'
		else 'NA'
	end gender,
	case
		when p.birthdate is null then '0'
		when p.birthdate is not null then extract (year
	from
		(age(date(p.birthdate))))
		else '0'
	end patient_age,
	case
		when extract (year
	from
		(age(date(p.birthdate)))) >= 0
		and extract (year
	from
		(age(date(p.birthdate))))<= 4 then '0_to_4'
		when extract (year
	from
		(age(date(p.birthdate)))) >= 5
		and extract (year
	from
		(age(date(p.birthdate)))) <= 9 then '5_to_9'
		when extract (year
	from
		(age(date(p.birthdate)))) >= 10
		and extract (year
	from
		(age(date(p.birthdate)))) <= 14 then '10_to_14'
		when extract (year
	from
		(age(date(p.birthdate)))) >= 15
		and extract (year
	from
		(age(date(p.birthdate)))) <= 19 then '15_to_19'
		when extract (year
	from
		(age(date(p.birthdate)))) >= 20 then '20_and_above'
		else 'unknown'
	end age_bracket,
	max(date(e.visit_date)) visit_date
from
	encounters e
left join people p on
	e.person_id = p.person_id
left join appointments a on
	e.encounter_id = a.encounter_id
join (
	select
		s.site_id,
		s.site_name facility_name,
		l.name district,
		pp.prime_partner
	from
		sites s
	join locations l on
		s.parent_district = l.location_id
	join prime_partners pp on
		s.partner_code = pp.partner_code) loc on
	cast(trim(leading '0' from right(cast(e.person_id as text), 5)) as integer) = cast(loc.site_id as integer)
where
	e.voided = '0'
	and a.voided = '0'
	and e.program_id = 1
	and a.concept_id = 5373
	and p.voided = '0'
	and loc.facility_name in (
	select
		site_name
	from
		ids.succesful_con_sites)
	and date(appointment_date) between (
	select
		cast(date_trunc('quarter', current_date) as date)) and (
	select
		date(cast((current_date) as date)::date - cast('1 days' as interval)))
group by
	loc.district,
	cast(loc.facility_name as CHAR(255)),
	loc.prime_partner,
	date(a.appointment_date),
	e.person_id,
	p.gender,
	patient_age,
	age_bracket ;

select * from tx_appointments ta ;
select distinct facility_name from tx_appointments ta ;





drop table if exists tx_visits;
create table tx_visits as
select
	DISTINCT loc.district,
	cast(loc.facility_name as CHAR(255)) facility_name,
	loc.prime_partner,
	date(e.visit_date) visit_date,
	e.person_id,
       case when p.gender='0' then 'F' 
	     when p.gender ='1' then 'M'
	     when p.gender is null then 'NA'
	     else 'NP'
	     end gender,
	case when p.birthdate is null then '0'
	     when p.birthdate is not null then
	       extract (year from (age(date(p.birthdate))))
	      else '0'
	      end patient_age,
      	 case when extract (year from (age(date(p.birthdate)))) >=0 and extract (year from (age(date(p.birthdate))))<=4 then '0_to_4'
	       when extract (year from (age(date(p.birthdate)))) >=5 and extract (year from (age(date(p.birthdate)))) <=9 then '5_to_9'
	       when extract (year from (age(date(p.birthdate)))) >=10 and extract (year from (age(date(p.birthdate)))) <=14 then '10_to_14'
	       when extract (year from (age(date(p.birthdate)))) >=15 and extract (year from (age(date(p.birthdate)))) <=19 then '15_to_19'
	       when extract (year from (age(date(p.birthdate)))) >=20 then '20_and_above'
	       else 'unknown'
	       end age_bracket,   
	case
		when appts.appt_status = '1' then 'had appointment in quarter'
		else 'had no appointment within quarter'
	end as appt_scheduled,
	'visit occurred within the quarter' as period_of_visit
from
	encounters e
left join people p on e.person_id=p.person_id
left join (
	select
		distinct appts1.person_id person,
		case
			when appts1.appointment_status = 'appointment within quarter' then '1'
			else 0
		end as appt_status
	from
		(
		select
			e2.person_id person_id ,
			a2.appointment_date,
			case
				when date(a2.appointment_date) between (
				SELECT
					CAST(date_trunc('quarter', current_date) as date)) and (
				SELECT
					CAST(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' AS date)) then 'appointment within quarter'
				else 'not within quarter'
			end appointment_status
		from
			encounters e2
		join appointments a2 on
			e2.encounter_id = a2.encounter_id
		where
			e2.program_id = '1'
			and e2.voided = '0'
			and a2.concept_id = '5373'
			and a2.voided = '0'
			and a2.appointment_date between (
			SELECT
				CAST(date_trunc('quarter', current_date) as date)) and (
			SELECT
				CAST(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' AS date)) ) appts1
	group by
		person,
		appointment_status )appts on
	e.person_id = appts.person
join (
	select
		s.site_id,
		cast(s.site_name as CHAR(255)) facility_name,
		l.name district,
		pp.prime_partner
	from
		sites s
	join locations l on
		s.parent_district = l.location_id
	join prime_partners pp on
		s.partner_code = pp.partner_code) loc on
	cast(trim(leading '0' from right(cast(e.person_id as text), 5)) as integer) = cast(loc.site_id as integer)
where
	e.voided = '0'
	and e.program_id = '1'
	and e.encounter_type_id = 81
	and loc.facility_name in (select site_name from ids.succesful_con_sites)
	and date(visit_date) between (
	SELECT
		CAST(date_trunc('quarter', current_date) as date)) and (select date(CAST(( current_date) as date)::date - cast('1 days' as interval)))
UNION ALL
select
	distinct loc.district,
	cast(loc.facility_name as CHAR(255)) facility_name,
	loc.prime_partner,
	date(e.visit_date) visit_date,
	e.person_id,
        	case when p.gender='0' then 'F' 
	     when p.gender ='1' then 'M'
	     when p.gender is null then 'NA'
	     else 'NP'
	     end gender,
	case when p.birthdate is null then '0'
	     when p.birthdate is not null then
	       extract (year from (age(date(p.birthdate))))
	      else '0'
	      end patient_age,
       	  case when extract (year from (age(date(p.birthdate)))) >=0 and extract (year from (age(date(p.birthdate))))<=4 then '0_to_4'
	       when extract (year from (age(date(p.birthdate)))) >=5 and extract (year from (age(date(p.birthdate)))) <=9 then '5_to_9'
	       when extract (year from (age(date(p.birthdate)))) >=10 and extract (year from (age(date(p.birthdate)))) <=14 then '10_to_14'
	       when extract (year from (age(date(p.birthdate)))) >=15 and extract (year from (age(date(p.birthdate)))) <=19 then '15_to_19'
	       when extract (year from (age(date(p.birthdate)))) >=20 then '20_and_above'
	       else 'unknown'
	       end age_bracket,   
	'had appointment in quarter' as appt_scheduled,
	'visit before appointment and quarter' as period_of_visit
from
	encounters e
left join people p on e.person_id=p.person_id
join (
	select
		DISTINCT loc.district,
		cast(loc.facility_name as CHAR(255)) facility_name,
		loc.prime_partner,
		date(a.appointment_date) appointment_date,
		e.person_id,
		date(e.visit_date) visit_date
	from
		encounters e
	left join appointments a on
		e.encounter_id = a.encounter_id
	join (
		select
			s.site_id,
			s.site_name facility_name,
			l.name district,
			pp.prime_partner
		from
			sites s
		join locations l on
			s.parent_district = l.location_id
		join prime_partners pp on
			s.partner_code = pp.partner_code) loc on
		cast(trim(leading '0' from right(cast(e.person_id as text), 5)) as integer) = cast(loc.site_id as integer)
	where
		e.voided = '0'
		and a.voided = '0'
		and e.program_id = 1
		and a.concept_id = 5373
		and loc.facility_name in (select site_name from ids.succesful_con_sites)
		and date(appointment_date) between (
		SELECT
			CAST(date_trunc('quarter', current_date) as date)) and (select date(CAST(( current_date) as date)::date - cast('1 days' as interval))) ) tap on
	(e.person_id = tap.person_id
		and date(e.visit_date)>date(tap.visit_date))
join (
	select
		s.site_id,
		cast(s.site_name as CHAR(255)) facility_name,
		l.name district,
		pp.prime_partner
	from
		sites s
	join locations l on
		s.parent_district = l.location_id
	join prime_partners pp on
		s.partner_code = pp.partner_code) loc on
	cast(trim(leading '0' from right(cast(e.person_id as text), 5)) as integer) = cast(loc.site_id as integer)
where
	e.person_id not in (
	select
		DISTINCT e.person_id
	from
		encounters e
	left join (
		select
			distinct appts1.person_id person,
			case
				when appts1.appointment_status = 'appointment within quarter' then '1'
				else 0
			end as appt_status
		from
			(
			select
				e2.person_id person_id ,
				a2.appointment_date,
				case
					when date(a2.appointment_date) between (
					SELECT
						CAST(date_trunc('quarter', current_date) as date)) and (
					SELECT
						CAST(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' AS date)) then 'appointment within quarter'
					else 'not within quarter'
				end appointment_status
			from
				encounters e2
			join appointments a2 on
				e2.encounter_id = a2.encounter_id
			where
				e2.program_id = '1'
				and e2.voided = '0'
				and a2.concept_id = '5373'
				and a2.voided = '0'
				and a2.appointment_date between (
				SELECT
					CAST(date_trunc('quarter', current_date) as date)) and (
				SELECT
					CAST(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' AS date)) ) appts1
		group by
			person,
			appointment_status )appts on
		e.person_id = appts.person
	where
		e.voided = '0'
		and e.program_id = '1'
		and e.encounter_type_id = 81
		and date(visit_date) between (
		SELECT
			CAST(date_trunc('quarter', current_date) as date)) and (select date(CAST(( current_date) as date)::date - cast('1 days' as interval))) )
	and e.voided = '0'
	and e.encounter_type_id = 81
	and e.program_id = 1
	and loc.facility_name in (select site_name from ids.succesful_con_sites)
	and date(e.visit_date) between (
	select
		date((
		SELECT
			CAST(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
	SELECT
		CAST(date_trunc('quarter', current_date) as date));








drop table if exists tx_visits_without_appointments;

create table tx_visits_without_appointments as
select
	distinct bv.district,
	bv.facility_name,
	bv.prime_partner,
	bv.person_id,
	bv.visit_date,
	date(bv.appointment_before_visit) appointment_before_visit,
	av.appointment_after_visit appointment_after_visit
from
	(
	select
		tv.district,
		tv.facility_name,
		tv.prime_partner,
		tv.person_id,
		tv.visit_date,
		case
			when max(date(before_q.appointment_date)) is null then '1900-12-31'
			else max(date(before_q.appointment_date))
		end appointment_before_visit
	from
		tx_visits tv
	left join (
		select
			enc.encounter_id,
			enc.program_id,
			enc.person_id,
			enc.visit_date,
			a.appointment_date
		from
			encounters enc
		join appointments a on
			enc.encounter_id = a.encounter_id
		where
			a.concept_id = '5373'
			and a.voided = '0'
			and enc.voided = '0'
			and enc.program_id = '1'
		order by
			enc.visit_date desc ) before_q on
		(before_q.person_id = tv.person_id
			and date(before_q.visit_date)<tv.visit_date)
	where
		tv.person_id not in (
		select
			distinct ta.person_id
		from
			tx_appointments ta)
	group by
		tv.district,
		tv.facility_name,
		tv.prime_partner,
		tv.person_id,
		tv.visit_date ) bv
left join (
	select
		tv.district,
		tv.facility_name,
		tv.prime_partner,
		tv.person_id,
		tv.visit_date,
		case
			when after_q.appointment_date is null then '1900-12-31'
			else date(after_q.appointment_date)
		end appointment_after_visit
	from
		tx_visits tv
	join (
		select
			enc.encounter_id,
			enc.program_id,
			enc.person_id,
			enc.visit_date,
			a.appointment_date
		from
			encounters enc
		join appointments a on
			enc.encounter_id = a.encounter_id
		where
			a.concept_id = '5373'
			and a.voided = '0'
			and enc.voided = '0'
			and enc.program_id = '1'
		order by
			enc.visit_date desc ) after_q on
		(after_q.person_id = tv.person_id
			and date(after_q.visit_date)= tv.visit_date)
	where
		tv.person_id not in (
		select
			distinct ta.person_id
		from
			tx_appointments ta) ) av on
	bv.person_id = av.person_id
	and bv.visit_date = av.visit_date ;





drop table if exists tx_visits_without_appointments_aggregated;
create table tx_visits_without_appointments_aggregated as
select tvwa.visit_date, tvwa.district, tvwa.facility_name,tvwa.prime_partner,count(tvwa.facility_name) total_vwa 
from tx_visits_without_appointments tvwa
group by tvwa.visit_date, tvwa.district, tvwa.facility_name,tvwa.prime_partner;




drop table if exists tx_missed_appointments;
create table tx_missed_appointments as
select distinct 
ma.recent_appointment recent_appointment,
ma.district,
ma.facility_name,
ma.prime_partner,
count(ma.facility_name) missed_appointments
from	
        (
        select distinct
        district,
		facility_name,
		prime_partner ,
		person_id,
		min(appointment_date) recent_appointment,
	    max(visit_date) visit_date
	from
		tx_appointments txa
	where
		txa.person_id not in (
		select
			distinct person_id
		from
			tx_visits
			)
	group by
		district,
		facility_name,
		prime_partner,
		person_id) ma
	group by 
ma.recent_appointment,
ma.district,
ma.facility_name,
ma.prime_partner;




drop table if exists tx_overdue_appointments;
create table tx_overdue_appointments as
select distinct
(select date(CAST(( current_date) as date)::date - cast('1 days' as interval))) as Report_date,
    dii.identifier De_identified_identifier,
     DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) Number_of_days_Overdue,
     CASE WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) < 14 THEN 'overdue_less_than_14_days'
     WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) = 14 THEN 'overdue_14_days'
     WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) >14 AND DATE_PART('day', now() - ma.recent_appointment) <28 THEN 'overdue_15_to_27_days'
     WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) = 28 THEN 'overdue_28_days'
     WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) >28 AND DATE_PART('day', now() - ma.recent_appointment)<60 THEN 'overdue_29_to_59_days'
     WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) =60 THEN 'overdue_60_days'
     WHEN DATE_PART('day', (select TIMESTAMP 'yesterday') - ma.recent_appointment) > 60 THEN 'overdue_Over_60_days'
     ELSE '' 
     END Overdue_kpi,
     ma.facility_name Facility_name,
     ma.district District,
     ma.prime_partner Partner,
     ma.recent_appointment date_appointment_missed
from	
        (
        select distinct
        district,
		facility_name,
		prime_partner ,
		person_id,
		min(appointment_date) recent_appointment,
	    max(visit_date) visit_date
	from
		tx_appointments txa
	where
		txa.person_id not in (
		select
			distinct person_id
		from
			tx_visits
			)
	group by
		district,
		facility_name,
		prime_partner,
		person_id) ma join de_identified_identifiers dii on ma.person_id = dii.person_id 
		where dii.voided ='0';




drop table if exists tx_raw_aggregate_1;

create table tx_raw_aggregate_1 as
select
	fin.appointment_date,
	fin.district,
	fin.facility_name,
	fin.prime_partner,
	fin.all_appointments as " Appointments due",
	case when fin.all_recorded_visits is null then 0 else
	fin.all_visits end " Appointments done",
	case when fin.all_recorded_visits is null then 0 else
	fin.missed_appointments end as " Missed / overdue appointments",
	fin.visits_with_no_appt_in_Q
from
	(
	select
		pm.appointment_date,
		pm.district,
		pm.facility_name,
		pm.prime_partner,
		pm.all_appointments,
		pm.all_recorded_visits,
        case when (select count(*) from tx_visits tv where date(visit_date) = (pm.appointment_date) and pm.facility_name=facility_name) is null then 0
        else
		(case
			when (
			select
				missed_appointments
			from
				tx_missed_appointments
			where
				recent_appointment = pm.appointment_date
				and pm.facility_name = facility_name) is null then 0
			else (
			select
				missed_appointments
			from
				tx_missed_appointments
			where
				recent_appointment = pm.appointment_date
				and pm.facility_name = facility_name)end)
		end missed_appointments,
		case when (select count(*) from tx_visits tv where visit_date = pm.appointment_date and pm.facility_name=facility_name) is null then 0
		else
		(pm.all_appointments-
		(case when (
		select
			missed_appointments
		from
			tx_missed_appointments
		where
			recent_appointment = pm.appointment_date
			and pm.facility_name = facility_name) is null then 0
		else (
		select
			missed_appointments
		from
			tx_missed_appointments
		where
			recent_appointment = pm.appointment_date
			and pm.facility_name = facility_name)
	end))end  all_visits,
		case
			when (pm.all_recorded_visits-(pm.all_appointments-(
			select
				missed_appointments
			from
				tx_missed_appointments
			where
				recent_appointment = pm.appointment_date
				and pm.facility_name = facility_name))) < 0
			or (pm.all_recorded_visits-(pm.all_appointments-(
			select
				missed_appointments
			from
				tx_missed_appointments
			where
				recent_appointment = pm.appointment_date
				and pm.facility_name = facility_name))) is null then 0
			else (pm.all_recorded_visits-(pm.all_appointments-(
			select
				missed_appointments
			from
				tx_missed_appointments
			where
				recent_appointment = pm.appointment_date
				and pm.facility_name = facility_name)))
		end as visits_with_no_appt_in_Q
	from
		(
		select
			ap.appointment_date,
			ap.district,
			ap.facility_name,
			ap.prime_partner,
			ap.all_appointments,
			case
				when vs.all_recorded_visits is null then 0
				else vs.all_recorded_visits
			end all_recorded_visits
		from
			((
			select
				appointment_date,
				district,
				facility_name,
				prime_partner,
				count(person_id) as all_appointments
			from
				tx_appointments
			group by
				appointment_date,
				district,
				facility_name,
				prime_partner) ap
		left join (
			select
				visit_date,
				district,
				facility_name,
				prime_partner,
				count(person_id) as all_recorded_visits
			from
				tx_visits
			group by
				visit_date,
				district,
				facility_name,
				prime_partner) vs on
			vs.visit_date = ap.appointment_date
			and vs.district = ap.district
			and vs.facility_name = ap.facility_name
			and vs.prime_partner = ap.prime_partner)) pm
	group by
		pm.appointment_date,
		pm.district,
		pm.facility_name,
		pm.prime_partner,
		pm.all_appointments,
		pm.all_recorded_visits,
		visits_with_no_appt_in_Q) fin
order by
	fin.facility_name,
	fin.appointment_date ;





drop table if exists tx_raw_aggregate_2;
create table tx_raw_aggregate_2 as
select
	ma1.date_of_incident,
	ma1.district,
	ma1.facility_name,
	ma1.prime_partner,
	count(ma1.facility_name) filter (
	where ma1.classification = 'incomplete visit') as incomplete_visits,
	count(ma1.facility_name) filter (
	where ma1.classification = 'transferred out') as transferred_out,
	count(ma1.facility_name) filter (
	where ma1.classification = 'patient died') as deceased,
	((select tma.missed_appointments from tx_missed_appointments tma  where tma.recent_appointment = ma1.date_of_incident and tma.district =ma1.district and tma.facility_name =ma1.facility_name and tma.prime_partner = ma1.prime_partner)-
	((case when 
    (count(ma1.facility_name) filter (	where ma1.classification = 'incomplete visit')) is null then 0 else (count(ma1.facility_name) filter (	where ma1.classification = 'incomplete visit')) end)  + 
    (case when 
    (count(ma1.facility_name) filter (where ma1.classification = 'transferred out')) is null then 0 else (count(ma1.facility_name) filter (where ma1.classification = 'transferred out')) end) +
    (case when 
    (count(ma1.facility_name) filter (where ma1.classification = 'patient died')) is null then 0 else (count(ma1.facility_name) filter (where ma1.classification = 'patient died'))end) ))
     as missed_for_unknown_reason
from
	(
	select
		ma.district,
		ma.facility_name,
		ma.prime_partner,
		ma.person_id,
		ma.missed_appointment_date,
		date(enc.visit_date) date_of_incident,
		'incomplete visit' as classification
	from
		(
		select
			ta.district,
			ta.facility_name,
			ta.prime_partner ,
			ta.person_id,
			min(ta.appointment_date) missed_appointment_date
		from
			tx_appointments ta
		where
			ta.person_id not in (
			select
				person_id
			from
				tx_visits)
		group by
			ta.district,
			ta.facility_name,
			ta.prime_partner,
			ta.person_id) ma
	join encounters enc on
		(enc.person_id = ma.person_id
			and date(enc.visit_date)>ma.missed_appointment_date)
	where
		enc.voided = '0'
		and enc.encounter_type_id = '78'
union all
	select
		ma.district,
		ma.facility_name,
		ma.prime_partner,
		ma.person_id,
		ma.missed_appointment_date,
		date(enc.visit_date) date_of_incident,
		'transferred out' as classification
	from
		(
		select
			ta.district,
			ta.facility_name,
			ta.prime_partner ,
			ta.person_id,
			min(ta.appointment_date) missed_appointment_date
		from
			tx_appointments ta
		where
			ta.person_id not in (
			select
				person_id
			from
				tx_visits)
		group by
			ta.district,
			ta.facility_name,
			ta.prime_partner,
			ta.person_id) ma
	join encounters enc on
		(enc.person_id = ma.person_id
			and date(enc.visit_date)>ma.missed_appointment_date)
	where
		enc.voided = '0'
		and enc.encounter_type_id in ('138', '92' )
union all
	select
		ma.district,
		ma.facility_name,
		ma.prime_partner,
		ma.person_id,
		ma.missed_appointment_date,
		date(otc.start_date) date_of_incident,
		'patient died' as classification
	from
		(
		select
			ta.district,
			ta.facility_name,
			ta.prime_partner ,
			ta.person_id,
			min(ta.appointment_date) missed_appointment_date
		from
			tx_appointments ta
		where
			ta.person_id not in (
			select
				person_id
			from
				tx_visits)
		group by
			ta.district,
			ta.facility_name,
			ta.prime_partner,
			ta.person_id) ma
	join outcomes otc on
		(otc.person_id = ma.person_id
			and date(otc.start_date)<ma.missed_appointment_date)
	where
		otc.voided = '0'
		and otc.concept_id in ('3348', '3349' )
			and otc.start_date>(
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('365 days' as interval)))) ma1
group by
	ma1.date_of_incident,
	ma1.district,
	ma1.facility_name,
	ma1.prime_partner ;





drop table if exists tx_raw_aggregate_3;
create table tx_raw_aggregate_3 as
select report_date,	district,facility_name,partner prime_partner,
count(facility_name) filter (
	where overdue_kpi = 'overdue_14_days') as overdue_14_days,
count(facility_name) filter (
	where overdue_kpi = 'overdue_less_than_14_days') as overdue_less_than_14_days,
	count(facility_name) filter (
	where overdue_kpi = 'overdue_15_to_27_days') as overdue_15_to_27_days,
	count(facility_name) filter (
	where overdue_kpi = 'overdue_28_days') as overdue_28_days,
	count(facility_name) filter (
	where overdue_kpi = 'overdue_29_to_59_days') as overdue_29_to_59_days,
	count(facility_name) filter (
	where overdue_kpi = 'overdue_60_days') as overdue_60_days,
	count(facility_name) filter (
	where overdue_kpi = 'overdue_Over_60_days') as overdue_Over_60_days
from  tx_overdue_appointments_all_report_dates
group by report_date,	district,facility_name,partner;

 


drop table if exists tx_raw_aggregate_4;
create table tx_raw_aggregate_4 as 
select ta.appointment_date, ta.district, ta.facility_name, ta.prime_partner,
count(ta.facility_name) filter (where ta.age_bracket='0_to_4') as Ages_0_to_4,
count(ta.facility_name) filter (where ta.age_bracket='5_to_9') as Ages_5_to_9,
count(ta.facility_name) filter (where ta.age_bracket='10_to_14') as Ages_10_to_14,
count(ta.facility_name) filter (where ta.age_bracket='15_to_19') as Ages_15_to_19,
count(ta.facility_name) filter (where ta.age_bracket='20_and_above') as Ages_20_and_above,
count(ta.facility_name) filter (where ta.gender='M') as Male,
count(ta.facility_name) filter (where ta.gender='F') as Female
from tx_appointments ta 
group by ta.appointment_date, ta.district, ta.facility_name, ta.prime_partner;


drop table if exists tx_raw_aggregate_5;
create table tx_raw_aggregate_5 as 
select tv.visit_date ,tv.district,tv.facility_name,tv.prime_partner,
count(tv.facility_name) filter (where tv.age_bracket='0_to_4') as Ages_0_to_4,
count(tv.facility_name) filter (where tv.age_bracket='5_to_9') as Ages_5_to_9,
count(tv.facility_name) filter (where tv.age_bracket='10_to_14') as Ages_10_to_14,
count(tv.facility_name) filter (where tv.age_bracket='15_to_19') as Ages_15_to_19,
count(tv.facility_name) filter (where tv.age_bracket='20_and_above') as Ages_20_and_above,
count(tv.facility_name) filter (where tv.gender='M') as Male,
count(tv.facility_name) filter (where tv.gender='F') as Female
from tx_visits tv 
where tv.person_id in (select distinct ta.person_id from tx_appointments ta)
group by tv.visit_date ,tv.district,tv.facility_name,tv.prime_partner;



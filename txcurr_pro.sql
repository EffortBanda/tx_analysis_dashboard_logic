drop table tx_appointments ;
create table tx_appointments as
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
		CAST(date_trunc('quarter', current_date) as date)) and (select date(CAST(( current_date) as date)::date - cast('1 days' as interval))) ;

drop table if exists tx_visits;

create table tx_visits as
select
	DISTINCT loc.district,
	cast(loc.facility_name as CHAR(255)) facility_name,
	loc.prime_partner,
	date(e.visit_date) visit_date,
	e.person_id,
	case
		when appts.appt_status = '1' then 'had appointment in quarter'
		else 'had no appointment within quarter'
	end as appt_scheduled,
	'visit occurred within the quarter' as period_of_visit
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
	'had appointment in quarter' as appt_scheduled,
	'visit before appointment and quarter' as period_of_visit
from
	encounters e
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

drop table if exists tx_missed_appointments;
create table tx_missed_appointments as
select
	ma.recent_appointment,
	ma.district,
	ma.facility_name,
	ma.prime_partner,
	count(ma.facility_name) missed_appointments
from
	(
	select
		district,
		facility_name,
		prime_partner ,
		person_id,
		min(appointment_date) recent_appointment
	from
		tx_appointments
	where
		person_id not in (
		select
			person_id
		from
			tx_visits
			)
	group by
		district,
		facility_name,
		prime_partner,
		person_id ) ma
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
DATE_PART('day', now() - ma.recent_appointment) Number_of_days_Overdue,
ma.facility_name Facility_name,
ma.district District,
ma.prime_partner Partner,
ma.visit_date last_visit_date
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
		where dii.voided ='0'
;


drop table if exists tx_overdue_appointments_all_report_dates;

create table tx_overdue_appointments_all_report_dates as
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('1 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '1 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('1 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('1 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('1 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('2 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '2 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('2 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('2 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('2 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('2 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('3 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '3 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('3 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('3 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('3 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('3 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('4 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '4 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('4 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('4 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('4 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('4 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('5 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '5 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('5 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('5 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('5 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('5 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('6 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '6 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('6 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('6 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('6 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('6 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('7 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '7 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('7 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('7 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('7 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('7 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('8 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '8 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('8 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('8 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('8 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('8 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('9 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '9 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('9 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('9 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('9 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('9 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('10 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '10 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('10 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('10 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('10 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('10 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('11 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '11 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('11 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('11 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('11 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('11 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('12 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '12 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('12 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('12 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('12 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('12 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('13 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '13 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('13 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('13 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('13 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('13 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('14 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '14 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('14 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('14 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('14 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('14 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('15 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '15 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('15 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('15 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('15 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('15 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('16 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '16 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('16 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('16 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('16 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('16 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('17 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '17 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('17 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('17 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('17 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('17 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('18 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '18 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('18 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('18 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('18 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('18 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('19 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '19 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('19 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('19 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('19 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('19 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('20 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '20 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('20 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('20 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('20 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('20 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('21 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '21 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('21 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('21 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('21 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('21 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('22 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '22 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('22 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('22 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('22 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('22 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('23 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '23 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('23 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('23 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('23 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('23 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('24 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '24 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('24 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('24 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('24 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('24 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('25 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '25 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('25 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('25 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('25 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('25 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('26 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '26 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('26 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('26 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('26 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('26 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('27 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '27 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('27 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('27 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('27 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('27 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('28 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '28 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('28 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('28 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('28 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('28 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('29 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '29 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('29 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('29 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('29 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('29 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) )
union all
select
	distinct (
	select
		date(cast((current_date) as date)::date - cast('30 days' as interval))) as Report_date,
	dii.identifier De_identified_identifier,
	DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) Number_of_days_Overdue,
	case
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) < 14 then 'overdue_less_than_14_days'
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) = 14 then 'overdue_14_days'
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) >14
		and DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) <28 then 'overdue_15_to_27_days'
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) = 28 then 'overdue_28_days'
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) >28
		and DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment)<60 then 'overdue_29_to_59_days'
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) = 60 then 'overdue_60_days'
		when DATE_PART('day', (select current_date - interval '30 days') - ma.recent_appointment) > 60 then 'overdue_Over_60_days'
		else ''
	end Overdue_kpi,
	ma.facility_name Facility_name,
	ma.district District,
	ma.prime_partner Partner,
	ma.recent_appointment date_appointment_missed
from
	(
	select
		distinct txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id,
		min(txa.appointment_date) recent_appointment,
		max(txa.visit_date) visit_date
	from
		(
		select
			distinct loc.district,
			cast(loc.facility_name as CHAR(255)) facility_name,
			loc.prime_partner,
			date(a.appointment_date) appointment_date,
			e.person_id,
			max(date(e.visit_date)) visit_date
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('30 days' as interval)))
		group by
			loc.district,
			cast(loc.facility_name as CHAR(255)),
			loc.prime_partner,
			date(a.appointment_date),
			e.person_id ) txa
	group by
		txa.district,
		txa.facility_name,
		txa.prime_partner ,
		txa.person_id) ma
join de_identified_identifiers dii on
	ma.person_id = dii.person_id
where
	dii.voided = '0'
	and ma.person_id not in (
	select
		distinct e.person_id
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
				select
					cast(date_trunc('quarter', current_date) as date)) and (
				select
					cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
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
		and date(visit_date) between (
		select
			cast(date_trunc('quarter', current_date) as date)) and (
		select
			date(cast((current_date) as date)::date - cast('30 days' as interval)))
union all
	select
		distinct e.person_id
	from
		encounters e
	join (
		select
			distinct loc.district,
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
			and date(appointment_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('30 days' as interval))) ) tap on
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
			distinct e.person_id
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
						select
							cast(date_trunc('quarter', current_date) as date)) and (
						select
							cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) then 'appointment within quarter'
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
					select
						cast(date_trunc('quarter', current_date) as date)) and (
					select
						cast(date_trunc('quarter', current_date) + interval '3 months' - interval '1 day' as date)) ) appts1
			group by
				person,
				appointment_status )appts on
			e.person_id = appts.person
		where
			e.voided = '0'
			and e.program_id = '1'
			and e.encounter_type_id = 81
			and date(visit_date) between (
			select
				cast(date_trunc('quarter', current_date) as date)) and (
			select
				date(cast((current_date) as date)::date - cast('30 days' as interval))) )
			and e.voided = '0'
			and e.encounter_type_id = 81
			and e.program_id = 1
			and date(e.visit_date) between (
			select
				date((
				select
					cast(date_trunc('quarter', current_date) as date))::date - cast('31 days' as interval))) and (
			select
				cast(date_trunc('quarter', current_date) as date)) );

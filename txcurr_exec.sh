#! /bin/bash

#variable declarations
database='ids';
dbuser='effort';
password='passpass';

#Generate list of sites that had succesful nft pull the previous day
mysql --user=$dbuser --password=$password $database  << EOF
source /home/cdr-user/txcurr_campaign/txcurr_sites.sql;
EOF
echo "sites generated succesfully";

#Drop temporary table for sites in pg
psql -U effort ids < /home/cdr-user/txcurr_campaign/txcurr_pg_sites_drop.sql

#Load the afore-mentioned generated list of sites that had succesful nft pull the previous day into temporary table in pg
pgloader /home/cdr-user/txcurr_campaign/nft_succesful.load
echo "sites updated succesfully";

#Load tx_curr tables into pg ids
psql -U effort ids < /home/cdr-user/txcurr_campaign/tx_overdue_appointments_all_report_dates.sql
psql -U effort ids < /home/cdr-user/txcurr_campaign/txcurr_pro.sql

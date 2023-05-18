create or replace database cz_bank;
use cz_bank;

create or replace table district
(
  district_code int primary key,	
  district_name varchar(100),
  region varchar(100),
  no_of_inhabitants int,
  no_of_municipalities_with_inhabitants_less_499 int,	
  no_of_municipalities_with_inhabitants_500_btw_1999 int,	
  no_of_municipalities_with_inhabitants_2000_btw_9999 int,
  no_of_municipalities_with_inhabitants_more_10000 int,
  no_of_cities int,
  ratio_of_urban_inhabitants float,	
  average_salary int,
  unemployment_rate_95 float,
  unemployment_rate_96 float,
  no_of_entrepreneurs_per_1000_inhabitants int,	
  no_of_committed_crimes_2017 int,
  no_of_committed_crimes_2018 int
);

create or replace table account
(
  account_id int primary key,
  district_id int,
  New_Frequency varchar(40),
  New_Date date,
  Account_type varchar(100),
  Card_Assigned varchar(20),
  foreign key(account_id) references district(district_code)
);
 
create or replace table order_list
(
  order_id int primary key,
  account_id int,	
  Bank_to varchar(50),
  account_to int,
  amount float,
  foreign key(account_id) references account(account_id)
);

create or replace table loan
(
  loan_id int,	
  account_id int,
  amount int,
  duration int,
  payments int,
  date_new date,
  status_new varchar(40),
  foreign key(account_id) references account(account_id)
);

create or replace table card
(
  card_id int primary key,
  disp_id int,
  type_new char(20),	
  issued_date date,
  foreign key(disp_id) references disposition(disp_id)
);

create or replace table client
(
  client_id	int primary key,
  district_id int,
  birth_date date,
  sex char(20),
  foreign key(district_id) references district(district_code)
);

create or replace table disposition
(
  disp_id int primary key,
  client_id int,
  account_id int,
  type char(20),
  foreign key(account_id) references account(account_id),
  foreign key(client_id) references client(client_id)
);

create or replace table transactions
(
  trans_id int,
  account_id int,
  Date date,
  Type varchar(30),
  operation varchar(40),	
  amount int,
  balance float,
  Purpose varchar(40),
  bank varchar(45),
  account_partern_id int,
  foreign key(account_id) references account(account_id)
);

-- Creating Cloud Storage Integration in Snowflake and S3 Bucket

create or replace storage integration s3pipe_inte
type = external_stage
storage_provider = s3
storage_aws_role_arn = 'arn:aws:iam::543100438405:role/czebank_data_analyst'
enabled = true
storage_allowed_locations = ('s3://czebankfinancialdata/');

desc integration s3pipe_inte;

-- Creating Stage So that Snowflake can access data

create or replace stage czebankdata_stage
storage_integration = s3pipe_inte
url  = 's3://czebankfinancialdata/'
--credentials = (aws_key_id = '' aws_secret_keys = '')
file_format = CSV_CZEBANK;

list @czebankdata_stage;
show stages;

-- Creating snowflake czebank_pipe that recognises csv that are ingested from external stage and copies the data into existiing tables
-- The auto_ingest = true parameter specifies to read event notifications sent from an s3 bucket to an SQSS queue when new data is ready to load.

create or replace pipe czebank_pipe_district
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."DISTRICT"
from '@czebankdata_stage/District/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_account
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."ACCOUNT"
from '@czebankdata_stage/Account_data/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_trnxs
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."TRANSACTIONS"
from '@czebankdata_stage/Trnx/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_disposition
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."DISPOSITION"
from '@czebankdata_stage/Disposition/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_card
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."CARD"
from '@czebankdata_stage/Card/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_order
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."ORDER_LIST"
from '@czebankdata_stage/Order/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_loan
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."LOAN"
from '@czebankdata_stage/Loan/'
file_format = CSV_CZEBANK;

create or replace pipe czebank_pipe_client
auto_ingest = True as copy into "CZ_BANK"."PUBLIC"."CLIENT"
from '@czebankdata_stage/Client/'
file_format = CSV_CZEBANK;

show pipes;

-- After Uploading in S3 Bucket use pipe refresh to see if the data flow is happening or not then use count to check if data is present now we can see the table 
-- Also columns can be shuffled but not extra name column should be present which is not define in sql table definition

alter pipe czebank_pipe_district refresh;
alter pipe czebank_pipe_account refresh;
alter pipe czebank_pipe_trnxs refresh;
alter pipe czebank_pipe_disposition refresh;
alter pipe czebank_pipe_card refresh;
alter pipe czebank_pipe_order refresh;
alter pipe czebank_pipe_loan refresh;
alter pipe czebank_pipe_client refresh;

select count(*) from district;
select count(*) from account;
select count(*) from transactions;
select count(*) from disposition;
select count(*) from card;
select count(*) from order_list;
select count(*) from loan;
select count(*) from client;

select * from district;
select * from account;
select * from transactions;
select * from disposition;
select * from card;
select * from order_list;
select * from loan;
select * from client;

---- Data Cleaning Processes

select * from transactions where bank is null and year(date) = '2016'

select year(date) as txn_year, count(*) as tot_txns from transactions
where bank is null
group by 1
order by 1 desc;

update transactions 
set date = dateadd(year,1,date)
where year(date) = '2021';

update transactions 
set date = dateadd(year,1,date)
where year(date) = '2020';

update transactions 
set date = dateadd(year,1,date)
where year(date) = '2019';

update transactions 
set date = dateadd(year,1,date)
where year(date) = '2018';

update transactions 
set date = dateadd(year,1,date)
where year(date) = '2017';

update transactions 
set date = dateadd(year,1,date)
where year(date) = '2016';

select min(date), max(date) from transactions;

-- In 2022 we have launched a new bank as sky bank

select * from transactions where bank is null and year(date) = 2022;
update transactions
set bank = 'Sky Bank' where bank is null and year(date) = 2022;

-- In 2021 DBS bank launched

select * from transactions where bank is null and year(date) = 2021;
update transactions
set bank = 'DBS Bank' where bank is null and year(date) = 2021;

select * from transactions where bank is null and year(date) = 2020; -- no null

-- In 2019 Northern bank launched

select * from transactions where bank is null and year(date) = 2019;
update transactions
set bank = 'Northern Bank' where bank is null and year(date) = 2019;

-- In 2018 Southern bank launched

select * from transactions where bank is null and year(date) = 2018;
update transactions
set bank = 'Southern Bank' where bank is null and year(date) = 2018;

-- In 2019 ADB bank launched

select * from transactions where bank is null and year(date) = 2017;
update transactions
set bank = 'ADB Bank' where bank is null and year(date) = 2017;

select * from transactions where bank is null; -- no null value present now
select * from transactions;

-- CLeaning for card

select * from card;
select distinct(year(issued_date)) from card;

update card 
set issued_date = dateadd(year,1,issued_date)
where year(issued_date) = '2021';

update card 
set issued_date = dateadd(year,1,issued_date)
where year(issued_date) = '2020';

update card 
set issued_date = dateadd(year,1,issued_date)
where year(issued_date) = '2019';

update card 
set issued_date = dateadd(year,1,issued_date)
where year(issued_date) = '2018';

update card 
set issued_date = dateadd(year,1,issued_date)
where year(issued_date) = '2017';

update card 
set issued_date = dateadd(year,1,issued_date)
where year(issued_date) = '2016'; 

-- SQL ANALYSIS

-- Getting Age of Clients and adding column

select * from client;
alter table client add column Age int;

update client
set Age = datediff('Year',birth_date,'2022-12-31');

-- Finding Male and Female Client

select sum(case when sex = 'Male' then 1 else 0 end) as Male_Client,
sum(case when sex = 'Female' then 1 else 0 end) as Female_Client from client;

-- Percentage Of Male and Female Client

select sum(case when sex = 'Male' then 1 else 0 end)/count(*)*100.0 as Male_Client_Perc,
sum(case when sex = 'Female' then 1 else 0 end)/count(*)*100.0 as Female_Client_Perc 
from client;

-- demographic profile of the bank's clients and how does it vary across districts

select * from district;

create or replace table demographic_profile as
select c.district_id, d.district_name, d.average_salary, round(avg(c.Age),0) as Avg_Age,
sum(case when sex = 'Male' then 1 else 0 end) as Male_Client,
sum(case when sex = 'Female' then 1 else 0 end) as Female_Client, 
round((Female_Client/Male_Client)*100,2) as Male_Female_Ratio_Perc,
count(*) as Total_Client from client c
join district d on c.district_id = d.district_code
group by 1,2,3
order by 1;

select * from demographic_profile;

-- Every last month customer account getting transacted

create or replace table latest_txns_bal as
select ltd.*, txn.balance from transactions as txn join
( 
select account_id, year(date) as Txn_Year, month(date) as Txn_Month, 
max(date) as Latest_Txn_Date from transactions
group by 1,2,3
order by 1,2,3
) as ltd on txn.account_id = ltd.account_id and txn.date = ltd.latest_txn_date
where txn.type = 'Credit' -- assuming month end txn data is credit
order by txn.account_id, ltd.txn_year, ltd.txn_month;

select * from latest_txns_bal;

-- Banks performance with their detailed analysis year & month-wise.

select Latest_Txn_Date, count(*) as Tot_Txns from latest_txns_bal
group by 1
order by 2 desc;

-- Making KPI's for Bank

create or replace table bank_kpi as
select ltb.Txn_Year, ltb.Txn_Month, t.Bank,a.Account_Type,
count(distinct ltb.account_id) as Tot_Account,
count(distinct t.Trans_id) as Tot_Txns,
count(case when t.Type = 'Credit' then 1 end) as Deposit_Count,
count(case when t.Type = 'Withdrawal' then 1 end) as Withdrawal_Count,
sum(ltb.balance) as Tot_Balance,
round((Deposit_Count/Tot_Txns)*100,2) as Deposit_Perc,
round((Withdrawal_Count/Tot_Txns)*100,2) as Withdrawal_Perc,
nvl(Tot_Balance/Tot_Account,0) as Avg_Balance,
round(Tot_Txns/Tot_Account,0) as Tpa
from transactions as t join latest_txns_bal as ltb on t.account_id = ltb.account_id 
left join account as a on t.account_id = a.account_id
group by 1,2,3,4
order by 1,2,3,4;

select * from bank_kpi;

select Txn_Year, count(*) as Total from bank_kpi
group by 1
order by 2 desc;

select * from bank_kpi order by Txn_Year, Bank;

select distinct(Txn_Year), Bank, sum(Avg_Balance) as Tot_Avg_Balance from bank_kpi
group by 1,2
order by 3 desc;















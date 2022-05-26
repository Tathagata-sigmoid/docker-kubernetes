CREATE TABLE IF NOT EXISTS execution_log(execution_date timestamp);
insert into execution_log values(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata');
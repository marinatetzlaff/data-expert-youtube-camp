--A DDL for a hosts_cumulated table

create table hosts_cumulated  (
    host text,
    host_activity_datelist jsonb,
    date DATE,
    PRIMARY KEY(host, date)
);
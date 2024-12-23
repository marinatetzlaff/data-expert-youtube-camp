--A DDL for an user_devices_cumulated table

create table user_devices_cumulated  (
    user_id text,
    device_activity_datelist jsonb,
    date DATE,
    PRIMARY KEY(user_id, date)
);


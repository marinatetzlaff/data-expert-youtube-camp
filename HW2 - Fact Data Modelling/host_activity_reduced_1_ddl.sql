--A DDL for a host_activity_reduced table
create table host_activity_reduced  (
    host text,
    month DATE,
    hit_array INTEGER[],
    unique_visitors_array INTEGER[],
    PRIMARY KEY(host, month)
);


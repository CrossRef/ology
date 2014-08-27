-- Raw input. One line per resolution. Temporarily populated and emptied into resolutions_aggregate.
create table resolutions (date date, doi varchar(1024), subdomain varchar(128), domain varchar(128), etld varchar(128));

-- Aggregated. One line per (doi, subdomain, domain, etld) per day.
create table resolutions_date_aggregate (date date, doi varchar(1024), subdomain varchar(128), domain varchar(128), etld varchar(128), y int, m int, d int, count int);
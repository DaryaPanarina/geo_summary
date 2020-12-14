```sql
CREATE USER geo_sum_update WITH PASSWORD 'password';


CREATE DATABASE geo_sum 
WITH OWNER "geo_sum_update" 
ENCODING 'UTF8' 
LC_COLLATE = 'en_US.UTF-8' 
LC_CTYPE = 'en_US.UTF-8';


CREATE SCHEMA geo_sum_update;


CREATE TABLE geo_sum_update.geo_summary (
	uid bigserial PRIMARY KEY,
	device_id bigint NOT NULL,
	last_location geometry(point, 4326) NOT NULL,
	address jsonb NOT NULL,
	speed smallint NOT NULL CHECK(speed >= 0),
	last_location_time timestamp(6) NOT NULL,
	check_time timestamp(6) NOT NULL DEFAULT now(),
	timezone_shift numeric(4, 2) NOT NULL
);
```

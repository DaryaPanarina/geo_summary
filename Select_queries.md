1. Все устройства, последнее местоположение которых было в заданном населенном пункте ("city_name").

```sql
SELECT device_id, max(last_location_time) last_location_time 
FROM geo_summary 
WHERE address @> '{"city":"city_name"}' 
GROUP BY device_id ORDER BY device_id;
```

2. Все устройства, последнее местоположение которых было на заданной улице/проспекте ("street_name").

```sql
SELECT device_id, max(last_location_time) last_location_time 
FROM geo_summary 
WHERE address @> '{"street":"street_name"}' 
GROUP BY device_id ORDER BY device_id;
```

3. Все устройства, последнее местоположение которых было в заданной геозоне, описываемой координатой ее центра (X - долгота, Y - широта) и радиусом (R метров).

```sql
SELECT DISTINCT ON (device_id) device_id FROM geo_summary
WHERE ST_DWithin(Geography(last_location), 
 Geography(ST_GeomFromEWKT('SRID=4326; POINT(X Y)')), R)
ORDER BY device_id, last_location_time;
```

4. Все уникальные адреса с количеством устройств, для которых первое местоположение было зафиксировано по этому адресу.

```sql
SELECT t.address, count(t.device_id) device_cnt FROM 
 (SELECT DISTINCT ON (device_id) device_id, address
  FROM geo_summary
  ORDER BY device_id, last_location_time) AS t
GROUP BY t.address
ORDER BY device_cnt DESC;
```

5. Все устройства, первое местоположение которых было зафиксировано по заданному адресу. В адресе указываются: название населенного пункта ("city_name"), название улицы/проспекта ("street_name") и номер дома ("house_number").

```sql
SELECT DISTINCT ON (device_id) device_id
FROM geo_summary
WHERE address @> '{
    "city": "city_name",
    "street": "street_name",
    "housenumber": "house_number"
}'
ORDER BY device_id, last_location_time;
```

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
SELECT t1.device_id FROM geo_summary t1 JOIN
(SELECT device_id, min(last_location_time) last_location_time 
 FROM geo_summary
 GROUP BY device_id) t2 
ON t1.device_id=t2.device_id AND t1.last_location_time=t2.last_location_time 
WHERE ST_DWithin(Geography(t1.last_location), 
 Geography(ST_GeomFromEWKT('SRID=4326; POINT(X Y)')), R)
ORDER BY t1.device_id;
```

4. Все уникальные адреса с количеством устройств, для которых первое местоположение было зафиксировано по этому адресу.

```sql
SELECT t1.address, count(t1.device_id) device_cnt FROM geo_summary t1 JOIN
(SELECT device_id, min(last_location_time) last_location_time 
 FROM geo_summary
 GROUP BY device_id) t2 
ON t1.device_id=t2.device_id AND t1.last_location_time=t2.last_location_time 
GROUP BY t1.address
ORDER BY device_cnt DESC;
```

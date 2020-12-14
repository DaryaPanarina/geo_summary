1. Все устройства, последнее местоположение которых было в заданном населенном пункте ("city_name").

```sql
SELECT device_id, max(last_location_time) last_location_time FROM geo_summary WHERE address @> '{"city":"city_name"}' 
GROUP BY device_id ORDER BY device_id;
```

2. Все устройства, последнее местоположение которых было на заданной улице/проспекте ("street_name").

```sql
SELECT device_id, max(last_location_time) last_location_time FROM geo_summary WHERE address @> '{"street":"street_name"}' 
GROUP BY device_id ORDER BY device_id;
```

3. 

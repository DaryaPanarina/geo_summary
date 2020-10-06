import sys
import Conntection as con


def insert_first_dev_locations():
    # Connections to databases and TimeZoneServer
    con_oracle = con.ConnectionOracle()
    if con_oracle.create_connection():
        print("Failed to connect to Oracle.")
        return -1
    con_psql = con.ConnectionPostgresql()
    if con_psql.create_connection():
        print("Failed to connect to PostgreSQL.")
        return -1
    con_postgis = con.ConnectionPostgis()
    if con_postgis.create_connection():
        print("Failed to connect to PostGIS.")
        return -1
    con_tz = con.ConnectionTimeZoneServer()
    if con_tz.create_connection():
        print("Failed to connect to TimeZoneServer.")
        return -1

    # Select data of the first position for each device
    if con_oracle.select_data(None):
        print("Failed to select data from Oracle.")
        return -2

    # Update geo_summary
    for device in con_oracle.selected_data:
        # Define address and timezone for each device
        if con_postgis.select_data(device[2], device[3]):
            print("Failed to define device's address.")
            return -3
        if con_tz.select_data(device[2], device[3], device[5]):
            print("Failed to define timezone.")
            return -4

        # Insert new row into geo_summary
        if con_psql.insert_data((device[0], device[1], device[2], device[3], con_postgis.selected_data, device[4],
                                 device[5], con_tz.selected_data)):
            print("Failed to insert new row into geo_summary.")
            return -5
    return len(con_oracle.selected_data)


def insert_last_dev_locations():
    # Connections to databases and TimeZoneServer
    con_mysql = con.ConnectionMysql()
    if con_mysql.create_connection():
        print("Failed to connect to MySQL.")
        return -1
    con_oracle = con.ConnectionOracle()
    if con_oracle.create_connection():
        print("Failed to connect to Oracle.")
        return -1
    con_redis = con.ConnectionRedis()
    if con_redis.create_connection():
        print("Failed to connect to Redis.")
        return -1
    con_psql = con.ConnectionPostgresql()
    if con_psql.create_connection():
        print("Failed to connect to PostgreSQL.")
        return -1
    con_postgis = con.ConnectionPostgis()
    if con_postgis.create_connection():
        print("Failed to connect to PostGIS.")
        return -1
    con_tz = con.ConnectionTimeZoneServer()
    if con_tz.create_connection():
        print("Failed to connect to TimeZoneServer.")
        return -1

    # Select list of all devices
    if con_mysql.select_data():
        print("Failed to select data from MySQL.")
        return -2

    # Define address and timezone for each device
    for device in con_mysql.selected_data:
        # Check if the device's location changed
        if con_redis.select_data(device[0]):
            print("Failed to select data from Redis.")
            return -2
        if con_psql.select_data(device[0]):
            print("Failed to select data from PostgreSQL.")
            return -2
        if con_redis.selected_data[0] == con_psql.selected_data:
            continue

        # Define address and timezone for each device
        if con_postgis.select_data(con_redis.selected_data[1], con_redis.selected_data[2]):
            print("Failed to define device's address.")
            return -3
        if con_tz.select_data(con_redis.selected_data[1], con_redis.selected_data[2], con_redis.selected_data[4]):
            print("Failed to define timezone.")
            return -4

        # Insert new row into geo_summary
        if con_oracle.select_data(con_redis.selected_data):
            print("Failed to select data from Oracle.")
            return -2
        if con_psql.insert_data((con_oracle.selected_data, device, con_redis.selected_data[1],
                                 con_redis.selected_data[2], con_postgis.selected_data, con_redis.selected_data[3],
                                 con_redis.selected_data[4], con_tz.selected_data)):
            print("Failed to insert new row into geo_summary.")
            return -5
    return len(con_oracle.selected_data)


if __name__ == '__main__':
    if '--first' in sys.argv:
        ans = insert_first_dev_locations()
    else:
        ans = insert_last_dev_locations()

    if ans < 0:
        sys.exit(ans)
    else:
        print(f"Inserted {ans} rows.")


import sys
import logging
from datetime import datetime
import time

import Conntection as con


def insert_first_dev_locations(logger):
    # Connect to databases and TimeZoneServer
    con_oracle = con.ConnectionOracle(logger)
    error = con_oracle.create_connection()
    if error:
        return (error,)
    con_psql = con.ConnectionPostgresql(logger)
    error = con_psql.create_connection()
    if error:
        return (error,)
    con_postgis = con.ConnectionPostgis(logger)
    error = con_postgis.create_connection()
    if error:
        return (error,)
    con_tz = con.ConnectionTimeZoneServer(logger)
    error = con_tz.create_connection()
    if error:
        return (error,)

    # Select data of the first position for each device
    start = time.time()
    error = con_oracle.select_data()
    logger.info(f"Oracle select data. Time: {time.time() - start}")
    if error:
        return (error,)

    # Update geo_summary
    errors_cnt = 0
    inserted_rows_cnt = 0
    # row = [device, lng, lat, speed, time]
    for row in con_oracle.selected_data:
        if row[0] is None:
            continue

        # Define address and timezone for each device
        if con_tz.select_data(row[1], row[2], datetime.timestamp(row[4])):
            errors_cnt += 1
            continue
        if con_postgis.select_data(row[1], row[2]):
            errors_cnt += 1
            continue

        # Insert new row into geo_summary
        # [device_id, lng, lat, address, speed, last_location_time, timezone_shift]
        speed = row[3]
        if speed is None:
            speed = 0
        if not con_psql.insert_data((row[0], row[1], row[2], con_postgis.selected_data, speed, datetime.timestamp(row[4]),
                                 con_tz.selected_data)):
            inserted_rows_cnt += 1
        else:
            errors_cnt += 1
    return errors_cnt, inserted_rows_cnt


def insert_last_dev_locations(logger):
    # Connections to databases and TimeZoneServer
    con_mysql = con.ConnectionMysql(logger)
    error = con_mysql.create_connection()
    if error:
        return [error]
    con_redis = con.ConnectionRedis(logger)
    error = con_redis.create_connection()
    if error:
        return [error]
    con_psql = con.ConnectionPostgresql(logger)
    error = con_psql.create_connection()
    if error:
        return [error]
    con_postgis = con.ConnectionPostgis(logger)
    error = con_postgis.create_connection()
    if error:
        return [error]
    con_tz = con.ConnectionTimeZoneServer(logger)
    error = con_tz.create_connection()
    if error:
        return [error]

    # Select list of all devices
    start = time.time()
    error = con_mysql.select_data()
    logger.info(f"MySQL select data. Time: {time.time() - start}")
    if error:
        return [error]

    # Check last location of each device
    errors_cnt = 0
    inserted_rows_cnt = 0
    unchanged_loc_cnt = 0
    # device = [device_id, ]
    for device in con_mysql.selected_data:
        # con_redis.selected_data = [device_id, lng, lat, speed, time]
        if con_redis.select_data(device[0]):
            errors_cnt += 1
            continue
        if con_psql.select_data(device[0]):
            errors_cnt += 1
            continue

        # Define device's timezone
        if con_tz.select_data(con_redis.selected_data[1], con_redis.selected_data[2], con_redis.selected_data[4]):
            errors_cnt += 1
            continue

        # Check if the device's location changed
        if (len(con_psql.selected_data) != 0) and (((con_redis.selected_data[1] == con_psql.selected_data[0][0])
                                                    and (con_redis.selected_data[2] == con_psql.selected_data[0][1]))
                                                   or ((con_redis.selected_data[4] + con_tz.selected_data * 3600)
                                                       <= con_psql.selected_data[0][2])):
            unchanged_loc_cnt += 1
            continue

        # Define device's address
        if con_postgis.select_data(con_redis.selected_data[1], con_redis.selected_data[2]):
            errors_cnt += 1
            continue

        # Insert new row into geo_summary
        # [device_id, lng, lat, address, speed, last_location_time, timezone_shift]
        if not con_psql.insert_data((device[0], con_redis.selected_data[1], con_redis.selected_data[2],
                                     con_postgis.selected_data, con_redis.selected_data[3],
                                     con_redis.selected_data[4], con_tz.selected_data)):
            inserted_rows_cnt += 1
        else:
            errors_cnt += 1
    return [errors_cnt, inserted_rows_cnt, unchanged_loc_cnt]


if __name__ == '__main__':
    # starting time
    start = time.time()

    logging.basicConfig(filename='geo_summary_error.log', filemode='w', format='[%(levelname)s]   %(message)s')
    logger = logging.getLogger("geo_summary")
    logger.setLevel('INFO')

    if '--first' in sys.argv:
        logger.info("Insert first devices' locations")
        ans = insert_first_dev_locations(logger)
    else:
        logger.info("Insert last devices' locations")
        ans = insert_last_dev_locations(logger)

    if ans[0] == -10:
        print("Failed to connect to database.")
        sys.exit(ans[0])
    elif ans[0] == -11:
        print("Failed to select data from database.")
        sys.exit(ans[0])
    else:
        if '--first' in sys.argv:
            ans_str = f"Inserted {ans[1]} rows. {ans[0]} errors occurred."
        else:
            ans_str = f"Inserted {ans[1]} rows. {ans[2]} devices haven't changed their location. " \
                      f"{ans[0]} errors occurred."

        logger.info(ans_str)
        print(ans_str)
    time_str = f"Runtime of the program is {time.time() - start}"
    logger.info(time_str)
    print(time_str)

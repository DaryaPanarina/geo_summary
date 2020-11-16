import sys
import argparse
import logging
from datetime import datetime
import time

import Connection as con


# Print iterations progress
def print_progress(iteration, total):
    percent = "{:.2f}".format((iteration / float(total)) * 100)
    print("Progress: {}% complete".format(percent), end="\r")
    # Print New Line on Complete
    if iteration == total:
        print()

def insert_first_dev_locations(config, logger):
    # Connect to databases and TimeZoneServer
    try:
        con_oracle = con.ConnectionOracle(config, logger)
        error = con_oracle.create_connection()
        if error:
            return (error,)
        con_psql = con.ConnectionPostgresql(config, logger)
        error = con_psql.create_connection()
        if error:
            return (error,)
        con_postgis = con.ConnectionPostgis(config, logger)
        error = con_postgis.create_connection()
        if error:
            return (error,)
        con_tz = con.ConnectionTimeZoneServer(config, logger)
        error = con_tz.create_connection()
        if error:
            return (error,)
    except Exception as e:
        logger.critical("Failed to read configuration file. The error occurred: {}.".format(e))
        return (-10,)

    # Select data of the first position for each device
    start = time.time()
    error = con_oracle.select_data()
    logger.info("Oracle select data. Time: {}.".format(time.time() - start))
    if error:
        return (error,)

    # Update geo_summary
    errors_cnt = 0
    inserted_rows_cnt = 0
    start = time.time()
    # row = [device, lng, lat, speed, time]
    for row in con_oracle.selected_data:
        # Print current progress
        print_progress(inserted_rows_cnt + errors_cnt, len(con_oracle.selected_data))

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
    print_progress(inserted_rows_cnt + errors_cnt, len(con_oracle.selected_data))
    logger.info("Processing data time: {}.".format(time.time() - start))
    return errors_cnt, inserted_rows_cnt


def insert_last_dev_locations(config, logger):
    # Connections to databases and TimeZoneServer
    try:
        con_mysql = con.ConnectionMysql(config, logger)
        error = con_mysql.create_connection()
        if error:
            return (error,)
        con_redis = con.ConnectionRedis(config, logger)
        error = con_redis.create_connection(None)
        if error:
            return (error,)
        con_psql = con.ConnectionPostgresql(config, logger)
        error = con_psql.create_connection()
        if error:
            return (error,)
        con_postgis = con.ConnectionPostgis(config, logger)
        error = con_postgis.create_connection()
        if error:
            return (error,)
        con_tz = con.ConnectionTimeZoneServer(config, logger)
        error = con_tz.create_connection()
        if error:
            return (error,)
    except Exception as e:
        logger.critical("Failed to read configuration file. The error occurred: {}.".format(e))
        return (-10,)

    # Select list of all devices
    start = time.time()
    error = con_mysql.select_data()
    logger.info("MySQL select data. Time: {}.".format(time.time() - start))
    if error:
        return (error,)

    # Check last location of each device
    errors_cnt = 0
    inserted_rows_cnt = 0
    unchanged_loc_cnt = 0
    start = time.time()
    # device = [device_id, ]
    for device in con_mysql.selected_data:
        # Print current progress
        print_progress(inserted_rows_cnt + unchanged_loc_cnt + errors_cnt, len(con_mysql.selected_data))

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
    print_progress(inserted_rows_cnt + unchanged_loc_cnt + errors_cnt, len(con_mysql.selected_data))
    logger.info("Processing data time: {}.".format(time.time() - start))
    return errors_cnt, inserted_rows_cnt, unchanged_loc_cnt


if __name__ == '__main__':
    # starting time
    start = time.time()

    # Parsing arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', default="config.yaml", type=str, help="path to configuration file", metavar="path")
    parser.add_argument('-f', '--first', action='store_true',
                        help="script defines address of the first location for each device")
    namespace = parser.parse_args(sys.argv[1:])

    logging.basicConfig(filename='geo_summary_error.log', filemode='w', format='[%(levelname)s]   %(message)s')
    logger = logging.getLogger("geo_summary")
    logger.setLevel('INFO')

    print("Start")
    if namespace.first:
        logger.info("Insert first devices' locations.")
        ans = insert_first_dev_locations(namespace.c, logger)
    else:
        logger.info("Insert last devices' locations.")
        ans = insert_last_dev_locations(namespace.c, logger)

    if ans[0] == -10:
        print("Failed to connect to database. Details are in geo_summary_error.log.")
        sys.exit(ans[0])
    elif ans[0] == -11:
        print("Failed to select data from database. Details are in geo_summary_error.log.")
        sys.exit(ans[0])
    else:
        if namespace.first:
            ans_str = "Inserted {} rows. {} errors occurred.".format(ans[1], ans[0])
        else:
            ans_str = "Inserted {} rows. {} devices haven't changed their location. " \
                      "{} errors occurred.".format(ans[1], ans[2], ans[0])

        logger.info(ans_str)
        print(ans_str)
    time_str = "Runtime of the program is {}.".format(time.time() - start)
    logger.info(time_str)
    print(time_str)

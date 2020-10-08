import sys
import logging

import Conntection as con


def insert_first_dev_locations(logger):
    # Connections to databases and TimeZoneServer
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
    error = con_oracle.select_data(None)
    if error:
        return (error,)

    # Update geo_summary
    errors_cnt = 0
    for row in con_oracle.selected_data:
        # Define address and timezone for each device
        if con_postgis.select_data(row[2], row[3]):
            errors_cnt += 1
            continue
        if con_tz.select_data(row[2], row[3], row[5]):
            errors_cnt += 1
            continue

        # Insert new row into geo_summary
        if con_psql.insert_data((row[0], row[1], row[2], row[3], con_postgis.selected_data, row[4], row[5],
                                 con_tz.selected_data)):
            errors_cnt += 1
            continue
    return len(con_oracle.selected_data) - errors_cnt, errors_cnt


def insert_last_dev_locations(logger):
    # Connections to databases and TimeZoneServer
    con_mysql = con.ConnectionMysql(logger)
    error = con_mysql.create_connection()
    if error:
        return (error,)
    con_oracle = con.ConnectionOracle(logger)
    error = con_oracle.create_connection()
    if error:
        return (error,)
    con_redis = con.ConnectionRedis(logger)
    error = con_redis.create_connection()
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

    # Select list of all devices
    if con_mysql.select_data():
        return (-2,)

    # Define address and timezone for each device
    errors_cnt = 0
    inserted_rows_cnt = 0
    for device in con_mysql.selected_data:
        # Check if the device's location changed
        if con_redis.select_data(device[0]):
            errors_cnt += 1
            continue
        if con_psql.select_data(device[0]):
            errors_cnt += 1
            continue
        if con_redis.selected_data[0] == con_psql.selected_data:
            continue

        # Define address and timezone for each device
        if con_postgis.select_data(con_redis.selected_data[1], con_redis.selected_data[2]):
            errors_cnt += 1
            continue
        if con_tz.select_data(con_redis.selected_data[1], con_redis.selected_data[2], con_redis.selected_data[4]):
            errors_cnt += 1
            continue

        # Insert new row into geo_summary
        if con_oracle.select_data(con_redis.selected_data):
            errors_cnt += 1
            continue
        if len(con_oracle.selected_data) == 0:
            logger.error(f"Device: {device[0]}. Oracle: no rows selected.")
            errors_cnt += 1
            continue
        if con_psql.insert_data((con_oracle.selected_data[0], device[0], con_redis.selected_data[1],
                                 con_redis.selected_data[2], con_postgis.selected_data, con_redis.selected_data[3],
                                 con_redis.selected_data[4], con_tz.selected_data)):
            errors_cnt += 1
            continue
        inserted_rows_cnt += 1
    return inserted_rows_cnt, errors_cnt


if __name__ == '__main__':
    logging.basicConfig(filename='geo_summary_error.log', filemode='w', format='[%(levelname)s]   %(message)s')
    logger = logging.getLogger("geo_summary")

    if '--first' in sys.argv:
        logger.info("Insert first devices' locations")
        ans = insert_first_dev_locations(logger)
    else:
        logger.info("Insert last devices' locations")
        ans = insert_last_dev_locations(logger)

    if ans[0] == -10:
        print("Failed to connect to database.")
        sys.exit(ans)
    elif ans[0] == -11:
        print("Failed to select data from database.")
        sys.exit(ans)
    else:
        ans_str = f"Inserted {ans[0]} rows. {ans[1]} errors occurred."
        logger.info(ans_str)
        print(ans_str)

import sys
import argparse
import logging
import time
from threading import Thread
import queue

import Connection as connections


# Connect to databases and TimeZoneServer
def init_connections(config, logger, isFirst):
    con = {}
    error = 0
    try:
        if isFirst:
            con['oracle'] = connections.ConnectionOracle(config, logger)
            error = con['oracle'].create_connection()
            if error:
                return {'error': error}
        else:
            con['redis'] = connections.ConnectionRedis(config, logger)
            error = con['redis'].create_connection()
            if error:
                return {'error': error}

        con['mysql'] = connections.ConnectionMysql(config, logger)
        error = con['mysql'].create_connection()
        if error:
            return {'error': error}
        con['psql'] = connections.ConnectionPostgresql(config, logger)
        error = con['psql'].create_connection()
        if error:
            return {'error': error}
        con['osm'] = connections.ConnectionOSM(config, logger)
        error = con['osm'].create_connection()
        if error:
            return {'error': error}
        con['tz'] = connections.ConnectionTimeZoneServer(config, logger)
        error = con['tz'].create_connection()
        if error:
            return {'error': error}
        return con
    except Exception as e:
        logger.critical("Failed to read configuration file. The error occurred: {}.".format(e))
        return {'error': -10}


# Print processing progress
def print_progress(progress, total):
    percent = "{:.2f}".format((progress / float(total)) * 100)
    print("Progress: {}% complete".format(percent), end="\r")


# Insert first location of each device in geo_summary
def insert_first_dev_locations(que, con, rows_range):
    offset = rows_range[0]
    chunk = 10
    errors_cnt = 0
    inserted_rows_cnt = 0
    max_time = 0
    while 1:
        # Select list of 10-19 devices
        if (offset + 2 * chunk) >= rows_range[1]:
            chunk = rows_range[1] - offset
        error = con['mysql'].select_data(offset, chunk)
        if error:
            que.put({'error': error})
            return

        for device in con['mysql'].selected_data:
            # Select data of the first location for each device
            # con['oracle'].selected_data = [lng, lat, speed, time]
            if con['oracle'].select_data(device):
                errors_cnt += 1
                continue
            if con['oracle'].selected_data[3] > time.time():
                logger.error("Device: {}. Incorrect timestamp.".format(device))
                errors_cnt += 1
                continue

            # Define device's timezone
            # args = (lng, lat, ts_utc)
            if con['tz'].select_data(con['oracle'].selected_data[0], con['oracle'].selected_data[1],
                                     con['oracle'].selected_data[3]):
                errors_cnt += 1
                continue

            # Define device's address
            # args = (lng, lat)
            if con['osm'].select_data(con['oracle'].selected_data[0], con['oracle'].selected_data[1]):
                errors_cnt += 1
                continue

            # Insert new row into geo_summary
            # args = (device_id, lng, lat, address, speed, last_location_time, timezone_shift)
            start_time = time.time()
            if not con['psql'].insert_data((device, con['oracle'].selected_data[0], con['oracle'].selected_data[1],
                                           con['osm'].selected_data, con['oracle'].selected_data[2],
                                           con['oracle'].selected_data[3], con['tz'].selected_data)):
                inserted_rows_cnt += 1
            else:
                errors_cnt += 1
            cur_time = time.time() - start_time
            if cur_time > max_time:
                max_time = cur_time
        # Print current progress
        que.put({'progress': chunk})
        offset += chunk
        if offset == rows_range[1]:
            break
    logger.info("Max processing time of one device: {}".format(max_time))
    que.put({'finish': (errors_cnt, inserted_rows_cnt, 0)})


# Check last location of each device
def insert_last_dev_locations(que, con, rows_range):
    offset = rows_range[0]
    chunk = 10
    errors_cnt = 0
    inserted_rows_cnt = 0
    unchanged_loc_cnt = 0
    max_time = 0

    while 1:
        # Select list of 10-19 devices
        if (offset + 2 * chunk) >= rows_range[1]:
            chunk = rows_range[1] - offset
        error = con['mysql'].select_data(offset, chunk)
        if error:
            que.put({'error': error})
            return

        # Select last locations of devices from geo_summary
        error = con['psql'].select_data(con['mysql'].selected_data)

        for device in con['mysql'].selected_data:
            start_time = time.time()
            # con['redis'].selected_data = [device_id, lng, lat, speed, time]
            if con['redis'].select_data(device):
                errors_cnt += 1
                continue
            if con['redis'].selected_data[4] > time.time():
                logger.error("Device: {}. Incorrect timestamp.".format(device))
                errors_cnt += 1
                continue

            # Check if the device's location changed
            if (not error) and (device in con['psql'].selected_data):
                if ((con['redis'].selected_data[1] == con['psql'].selected_data[device][1])
                        and (con['redis'].selected_data[2] == con['psql'].selected_data[device][2]))\
                        or (con['redis'].selected_data[4] <= con['psql'].selected_data[device][3]):
                    unchanged_loc_cnt += 1
                    continue

            # Define device's timezone
            # args = (lng, lat, ts_utc)
            if con['tz'].select_data(con['redis'].selected_data[1], con['redis'].selected_data[2],
                                     con['redis'].selected_data[4]):
                errors_cnt += 1
                continue

            # Define device's address
            # args = (lng, lat)
            if con['osm'].select_data(con['redis'].selected_data[1], con['redis'].selected_data[2]):
                errors_cnt += 1
                continue

            # Insert new row into geo_summary
            # [device_id, lng, lat, address, speed, last_location_time, timezone_shift]
            if not con['psql'].insert_data((device, con['redis'].selected_data[1], con['redis'].selected_data[2],
                                            con['osm'].selected_data, con['redis'].selected_data[3],
                                            con['redis'].selected_data[4], con['tz'].selected_data)):
                inserted_rows_cnt += 1
            else:
                errors_cnt += 1
            cur_time = time.time() - start_time
            if cur_time > max_time:
                max_time = cur_time

        # Print current progress
        que.put({'progress': chunk})
        offset += chunk
        if offset == rows_range[1]:
            break
    logger.info("Max processing time of one device: {}".format(max_time))
    que.put({'finish': (errors_cnt, inserted_rows_cnt, unchanged_loc_cnt)})


if __name__ == '__main__':
    start_time = time.time()

    # Parsing arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', default="config.yaml", type=str, help="path to configuration file", metavar="path")
    parser.add_argument('-f', '--first', action='store_true',
                        help="script defines address of the first location for each device")
    namespace = parser.parse_args(sys.argv[1:])

    logging.basicConfig(filename='geo_summary_error.log', filemode='w', format='[%(levelname)s]   %(message)s')
    logger = logging.getLogger("geo_sum_main")
    logger.setLevel('INFO')

    rows_number = 0
    threads_number = 15
    chunk_size = 0
    que = queue.Queue()
    threads_list = list()

    if namespace.first:
        print("Insert first devices' locations.")
        logger.info("Insert first devices' locations.")
    else:
        print("Insert last devices' locations.")
        logger.info("Insert last devices' locations.")
    print_progress(0, 100)

    for i in range(threads_number):
        con = init_connections(namespace.c, logger, namespace.first)

        if 'error' in con:
            print("Failed to connect to database. Details are in geo_summary_error.log.")
            sys.exit(con['error'])
        if i == 0:
            error = con['mysql'].select_data()
            if error:
                print("Failed to select data from database. Details are in geo_summary_error.log.")
                sys.exit(error)
            rows_number = con['mysql'].selected_data
            chunk_size = rows_number // threads_number

        if namespace.first:
            if i + 1 < threads_number:
                t = Thread(target=insert_first_dev_locations,
                           args=(que, con, (i * chunk_size, (i + 1) * chunk_size)), daemon=True)
            else:
                t = Thread(target=insert_first_dev_locations,
                           args=(que, con, (i * chunk_size, (i + 1) * chunk_size + rows_number % threads_number)),
                           daemon=True)
        else:
            if i + 1 < threads_number:
                t = Thread(target=insert_last_dev_locations,
                           args=(que, con, (i * chunk_size, (i + 1) * chunk_size)), daemon=True)
            else:
                t = Thread(target=insert_last_dev_locations,
                           args=(que, con, (i * chunk_size, (i + 1) * chunk_size + rows_number % threads_number)),
                           daemon=True)
        t.start()
        threads_list.append(t)

    progress = 0
    ans = [0, 0, 0]
    while (rows_number != progress) or (ans[0] + ans[1] + ans[2] != progress):
        result = que.get()
        if 'finish' in result:
            ans[0] += result['finish'][0]
            ans[1] += result['finish'][1]
            ans[2] += result['finish'][2]
        if 'progress' in result:
            progress += result['progress']
            print_progress(progress, rows_number)
        if 'error' in result:
            ans[0] = result['error']
            break

    if ans[0] < 0:
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
    time_str = "Runtime of the program is {:.3f} hours.".format((time.time() - start_time) / 3600)
    logger.info(time_str)
    print(time_str)

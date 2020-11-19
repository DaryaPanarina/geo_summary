import sys
import argparse
import logging
import time
from threading import Thread
import queue

import Connection as connections


# Connect to databases and TimeZoneServer
def init_connections(config, logger, mode):
    con = {}
    error = 0
    try:
        if mode:
            con['redis'] = connections.ConnectionRedis(config, logger)
            error = con['redis'].create_connection()
            if error:
                return {'error': error}

        con['mysql'] = connections.ConnectionMysql(config, logger)
        error = con['mysql'].create_connection()
        if error:
            return {'error': error}
        con['oracle'] = connections.ConnectionOracle(config, logger)
        error = con['oracle'].create_connection()
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
def print_progress(progress):
    print("Progress: {} devices were processed".format(progress), end="\r")


# Insert first location of each device in geo_summary
def insert_first_dev_locations(con, rows_range):
    cur_offset = rows_range[0]
    errors_cnt = 0
    inserted_rows_cnt = 0
    exec_time = 0
    while 1:
        # Print current progress
        print_progress(inserted_rows_cnt + errors_cnt)

        # Select list of 10 devices
        start = time.time()
        error = con['mysql'].select_data(cur_offset)
        if error:
            return (error,)
        if cur_offset >= rows_range[1]:
            break
        cur_offset = cur_offset + 10

        # device = [device_id, ]
        for device in con['mysql'].selected_data:
            # Select data of the first location for each device
            # con['oracle'].selected_data = [lng, lat, speed, time]
            cur_time = time.time()
            if con['oracle'].select_data(device[0]):
                errors_cnt += 1
                continue
            cur_time = time.time() - cur_time

            # Define timezone and address for each device
            # args = (lng, lat, ts_utc)
            if con['tz'].select_data(con['oracle'].selected_data[0], con['oracle'].selected_data[1],
                                     con['oracle'].selected_data[3]):
                errors_cnt += 1
                continue
            # args = (lng, lat)
            if con['osm'].select_data(con['oracle'].selected_data[0], con['oracle'].selected_data[1]):
                errors_cnt += 1
                continue

            # Insert new row into geo_summary
            # args = (device_id, lng, lat, address, speed, last_location_time, timezone_shift)
            if not con['psql'].insert_data((device[0], con['oracle'].selected_data[0], con['oracle'].selected_data[1],
                                           con['osm'].selected_data, con['oracle'].selected_data[2],
                                           con['oracle'].selected_data[3], con['tz'].selected_data)):
                inserted_rows_cnt += 1
            else:
                errors_cnt += 1
        exec_time += time.time() - start

    logger.info("Average time of one device processing: {}.".format(exec_time / (inserted_rows_cnt + errors_cnt)))
    return errors_cnt, inserted_rows_cnt

# Check last location of each device
def insert_last_dev_locations(con):
    cur_offset = 0
    errors_cnt = 0
    inserted_rows_cnt = 0
    unchanged_loc_cnt = 0
    exec_time = 0
    while 1:
        # Print current progress
        print_progress(inserted_rows_cnt + errors_cnt + unchanged_loc_cnt)

        # Select list of 10 devices
        start = time.time()
        error = con['mysql'].select_data(cur_offset)
        if error:
            return (error,)
        if len(con['mysql'].selected_data) == 0:
        # if cur_offset == 20:
            break
        cur_offset = cur_offset + 10

        # device = [device_id, ]
        for device in con['mysql'].selected_data:
            # con['redis'].selected_data = [device_id, lng, lat, speed, time]
            if con['redis'].select_data(device[0]):
                errors_cnt += 1
                continue
            if con['psql'].select_data(device[0]):
                errors_cnt += 1
                continue

            # Define device's timezone
            # args = (lng, lat, ts_utc)
            if con['tz'].select_data(con['redis'].selected_data[1], con['redis'].selected_data[2],
                                     con['redis'].selected_data[4]):
                errors_cnt += 1
                continue

            # Check if the device's location changed
            if (len(con['psql'].selected_data) != 0) \
                    and (((con['redis'].selected_data[1] == con['psql'].selected_data[0][0])
                          and (con['redis'].selected_data[2] == con['psql'].selected_data[0][1]))
                         or ((con['redis'].selected_data[4] + con['tz'].selected_data * 3600)
                             <= con['psql'].selected_data[0][2])):
                unchanged_loc_cnt += 1
                continue

            # Define device's address
            # args = (lng, lat)
            if con['osm'].select_data(con['redis'].selected_data[1], con['redis'].selected_data[2]):
                errors_cnt += 1
                continue

            # Insert new row into geo_summary
            # [device_id, lng, lat, address, speed, last_location_time, timezone_shift]
            if not con['psql'].insert_data((device[0], con['redis'].selected_data[1], con['redis'].selected_data[2],
                                            con['osm'].selected_data, con['redis'].selected_data[3],
                                            con['redis'].selected_data[4], con['tz'].selected_data)):
                inserted_rows_cnt += 1
            else:
                errors_cnt += 1
        exec_time += time.time() - start

    logger.info("Average time of one device processing: {}.".format(exec_time / (inserted_rows_cnt + errors_cnt)))
    print_progress(inserted_rows_cnt + unchanged_loc_cnt + errors_cnt)
    return errors_cnt, inserted_rows_cnt, unchanged_loc_cnt


if __name__ == '__main__':
    start = time.time()

    # Parsing arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', default="config.yaml", type=str, help="path to configuration file", metavar="path")
    parser.add_argument('-f', '--first', action='store_true',
                        help="script defines address of the first location for each device")
    namespace = parser.parse_args(sys.argv[1:])

    logging.basicConfig(filename='geo_summary_error.log', filemode='w', format='[%(levelname)s]   %(message)s')
    logger = logging.getLogger("geo_sum_main")
    logger.setLevel('INFO')

    print("Start")
    ans = ()
    if namespace.first:
        logger.info("Insert first devices' locations.")
        rows_number = 0
        threads_number = 15
        chunk_size = 0
        que = queue.Queue()
        threads_list = list()

        for i in range(threads_number):
            logger_i = logging.getLogger("geo_sum_{}".format(i))
            logger_i.setLevel('INFO')
            con = init_connections(namespace.c, logger_i, 0)
            if 'error' in con:
                print("Failed to connect to database. Details are in geo_summary_error.log.")
                sys.exit(con['error'])
            if i == 0:
                error = con['mysql'].select_data()
                if error:
                    print("Failed to select data from database. Details are in geo_summary_error.log.")
                    sys.exit(error)
                rows_number = con['mysql'].selected_data[0][0]
                chunk_size = rows_number // threads_number
            if i + 1 < threads_number:
                t = Thread(target=lambda q, f_args: q.put(insert_first_dev_locations(f_args[0], f_args[1])),
                           args=(que, [con, (i * chunk_size, (i + 1) * chunk_size)]))
            else:
                t = Thread(target=lambda q, f_args: q.put(insert_first_dev_locations(f_args[0], f_args[1])),
                           args=(que, [con, (i * chunk_size, (i + 1) * chunk_size + rows_number % threads_number)]))
            t.start()
            threads_list.append(t)

        # Join all the threads
        for t in threads_list:
            t.join()
        # Check thread's return value
        cur_ans = [0, 0]
        while not que.empty():
            result = que.get()
            cur_ans[0] += result[0]
            cur_ans[1] += result[1]
        ans = (cur_ans[0], cur_ans[1])
    else:
        logger.info("Insert last devices' locations.")
        con = init_connections(namespace.c, logger, 1)
        if 'error' in con:
            print("Failed to connect to database. Details are in geo_summary_error.log.")
            sys.exit(con['error'])
        ans = insert_last_dev_locations(con)

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
    time_str = "Runtime of the program is {}.".format(time.time() - start)
    logger.info(time_str)
    print(time_str)

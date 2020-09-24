import argparse
import Conntection as con

def insert_first_dev_locations():
    con_oracle = con.ConnectionOracle()
    if not con_oracle.create_connection():
        if not con_oracle.select_data():
            print("Oracle. Total number of rows is: ", len(con_oracle.selected_data), "\n")
            for row in con_oracle.selected_data:
                print("node_id = ", row[0], )
                print("lat = ", row[1])
                print("lng  = ", row[2])
                print("time  = ", row[3])
                print("device  = ", row[4])
                print("speed  = ", row[5])
                print("direction = ", row[6])
                print("sat_cnt  = ", row[7])
                print("accuracy = ", row[8])
                print("is_move = ", row[9], "\n")
        con_oracle.close_connection()

def insert_last_dev_locations():
    con_mysql = con.ConnectionMysql()
    if not con_mysql.create_connection():
        if not con_mysql.select_data():
            print("MySQl. Total number of rows is: ", len(con_mysql.selected_data), "\n")
            for row in con_mysql.selected_data:
                print("device_id = ", row[0])
                print("device_type = ", row[1])
                print("imei  = ", row[2])
                print("device_owner  = ", row[3])
                print("device_state  = ", row[4])
                print("last_time_activity  = ", row[5])
                print("device_serial  = ", row[6])
                print("extra_guarantee  = ", row[7], "\n")
        con_mysql.close_connection()

    con_psql = con.ConnectionPostgresql()
    if not con_psql.create_connection():
        values = (1, 1, 3.27, 3.27, '{"a": "asf", "b": "dgj", "c": "hkl"}', 1, '2011-05-16 15:36:38', 9.5)
        if not con_psql.insert_data(values):
            if not con_psql.select_data():
                print("PostgreSQL. Total number of rows is: ", len(con_psql.selected_data), "\n")
                for row in con_psql.selected_data:
                    print("uid = ", row[0], )
                    print("node_id = ", row[1])
                    print("device_id  = ", row[2])
                    print("last_position  = ", row[3])
                    print("address  = ", row[4])
                    print("speed  = ", row[5])
                    print("last_position_time = ", row[6])
                    print("check_time  = ", row[7])
                    print("timezone = ", row[8], "\n")
        con_psql.close_connection()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--first')
    args = parser.parse_args()

    if args.first:
        insert_first_dev_locations()
    else:
        insert_last_dev_locations()

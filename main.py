import Conntection as con


if __name__ == '__main__':
    con_mysql = con.ConnectionMysql()
    if not con_mysql.create_connection():
        con_mysql.select_data()
        print("Total number of rows is: ", len(con_mysql.selected_data), "\n")
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
        con_psql.select_data()
        print("Total number of rows is: ", len(con_psql.selected_data), "\n")
        for row in con_psql.selected_data:
            print("uid = ", row[0], )
            print("node_id = ", row[1])
            print("device_id  = ", row[2])
            print("first_position  = ", row[3])
            print("last_position  = ", row[4])
            print("address  = ", row[5])
            print("speed  = ", row[6])
            print("last_position_time = ", row[7])
            print("check_time  = ", row[8])
            print("timezone = ", row[9], "\n")
        con_psql.close_connection()

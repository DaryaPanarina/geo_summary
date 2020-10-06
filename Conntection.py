from abc import ABC, abstractmethod
import configparser
import base64

import mysql.connector
import cx_Oracle
import psycopg2
import redis
import requests

import gps_data_pb2


class Connection(ABC):
    dbms = ""

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self._host = config[self.dbms]["host"]
        self._port = config[self.dbms]["port"]
        if config.has_option(self.dbms, "user"):
            self._user = config[self.dbms]["user"]
        if config.has_option(self.dbms, "password"):
            self._password = config[self.dbms]["password"]
        if config.has_option(self.dbms, "database"):
            self._database = config[self.dbms]["database"]
        if config.has_option(self.dbms, "table"):
            self._table = config[self.dbms]["table"]
        self._connection = None
        self.selected_data = None

    @abstractmethod
    def create_connection(self):
        pass

    @abstractmethod
    def select_data(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass


class ConnectionMysql(Connection):
    def __init__(self):
        self.dbms = "MySQL"
        super().__init__()

    def __del__(self):
        if (self._connection is not None) and self._connection.is_connected():
            self._connection.close()

    def create_connection(self):
        try:
            self._connection = mysql.connector.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                passwd=self._password,
                database=self._database
            )
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -10

    def select_data(self):
        query = f"SELECT device_id FROM {self._table} LIMIT 10;"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def close_connection(self):
        if (self._connection is not None) and self._connection.is_connected():
            self._connection.close()


class ConnectionOracle(Connection):
    def __init__(self):
        self.dbms = "Oracle"
        super().__init__()

    def __del__(self):
        if self.is_open():
            self._connection.close()

    def create_connection(self):
        try:
            dsn_tns = cx_Oracle.makedsn(
                self._host,
                self._port,
                service_name=self._database
            )
            self._connection = cx_Oracle.connect(
                user=self._user,
                password=self._password,
                dsn=dsn_tns
            )
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -10

    def select_data(self, node_data):
        if node_data is None:
            query = f"SELECT DISTINCT nodes_2.node_id, nodes_2.device, nodes_2.lng, nodes_2.lat, nodes_2.speed, " \
                    f"nodes_2.time FROM nodes_2 INNER JOIN (SELECT device, min(time) time FROM nodes_2 " \
                    f"GROUP BY device ORDER BY device OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY) t " \
                    f"ON nodes_2.device=t.device AND nodes_2.time=t.time"
        else:
            query = f"SELECT node_id FROM {self._table} WHERE device={node_data[0]} AND lng={node_data[1]} " \
                    f"AND lat=={node_data[2]} AND speed=={node_data[3]} AND time={node_data[4]}"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def close_connection(self):
        if self.is_open():
            self._connection.close()

    def is_open(self):
        try:
            return self._connection.ping() is None
        except Exception:
            return False


class ConnectionPostgresql(Connection):
    def __init__(self):
        self.dbms = "PostgreSQL"
        super().__init__()

    def __del__(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()

    def create_connection(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._database
            )
            self._connection.autocommit = True
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -10

    def select_data(self, device_id):
        query = f"SELECT max(last_location_time) FROM {self._table} WHERE device_id={device_id};"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def insert_data(self, values):
        query = f"INSERT INTO {self._table} VALUES(DEFAULT, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s, " \
                f"TIMESTAMP %s, DEFAULT, %s)"
        print(query)
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, values)
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()


class ConnectionPostgis(Connection):
    def __init__(self):
        self.dbms = "PostGIS"
        super().__init__()

    def __del__(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()

    def create_connection(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._database
            )
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -10

    def select_data(self, lng, lat):
        query = f"SELECT name, street, housenumber, city, postcode FROM osm_buldings " \
                f"WHERE ST_DWithin(Geography(ST_Transform(ST_Centroid(geometry), 4326)), " \
                f"Geograohy(ST_SetSRID(ST_Point({lng}, {lat}), 4326)), 100) and name <>'' LIMIT 1;"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()


class ConnectionRedis(Connection):
    def __init__(self):
        self.dbms = "Redis"
        super().__init__()

    def __del__(self):
        if self.is_open():
            self._connection.close()

    def create_connection(self):
        try:
            self._connection = redis.Redis(
                host=self._host,
                port=self._port,
            )
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -10

    def select_data(self, device_id):
        name = "device:" + str(device_id) + ":last_pos"
        try:
            data = self._connection.hmget(name, ["data"])
            if len(data) != 1:
                return -13

            gps_str = base64.b64decode(data[0].decode("utf-8"))
            gps = gps_data_pb2.GPS()
            gps.ParseFromString(gps_str)

            lng = gps.lon_deg + float(f"0.{gps.lon_flt}")
            lat = gps.lat_deg + float(f"0.{gps.lat_flt}")
            self.selected_data = [device_id, lng, lat, gps.speed, gps.ts]
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def close_connection(self):
        if self.is_open():
            self._connection.close()

    def is_open(self):
        try:
            self._connection.ping()
            return True
        except Exception:
            return False


class ConnectionTimeZoneServer(Connection):
    def __init__(self):
        self.dbms = "TimeZoneServer"
        super().__init__()
        self._url = "http://" + self._host + ':' + str(self._port) + '/tz.json'

    def __del__(self):
        try:
            self._connection.close()
        except Exception:
            pass

    def create_connection(self):
        self._connection = requests.Session()

    def select_data(self, lng, lat, ts_utc):
        data = {"lon": lng, "lat": lat, "t": ts_utc}
        try:
            response = self._connection.get(self._url, data=data)
            json_data = response.json()
            self.selected_data = int(json_data['shift'])
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -11

    def close_connection(self):
        try:
            self._connection.close()
        except Exception:
            pass

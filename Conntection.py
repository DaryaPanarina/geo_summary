from abc import ABC, abstractmethod
import configparser
import base64
import json

import mysql.connector
import cx_Oracle
import psycopg2
import redis
import requests

# Protobuf structure GPS
import gps_data_pb2


class Connection(ABC):
    dbms = ""

    def __init__(self, logger):
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
        self._logger = logger

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
    def __init__(self, logger):
        self.dbms = "MySQL"
        super().__init__(logger)

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
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
            self._logger.error(f"Failed to connect to {self.dbms}. The error occurred: {e}")
            return -10

    def select_data(self):
        query = f"SELECT device_id FROM {self._table} LIMIT 10;"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = cursor.fetchall()
            return 0
        except Exception as e:
            self._logger.error(f"Failed to select data from {self.dbms}. The error occurred: {e}")
            self.selected_data = None
            return -11

    def close_connection(self):
        if (self._connection is not None) and self._connection.is_connected():
            self._connection.close()
        self.selected_data = None


class ConnectionOracle(Connection):
    def __init__(self, logger):
        self.dbms = "Oracle"
        super().__init__(logger)

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
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
            self._logger.error(f"Failed to connect to {self.dbms}. The error occurred: {e}")
            return -10

    def select_data(self, node_data):
        if node_data is None:
            query = f"SELECT a.node_id, a.device, a.lng, a.lat, a.speed, a.time FROM {self._table} a " \
                    f"INNER JOIN (SELECT device, min(time) time FROM {self._table} " \
                    f"GROUP BY device ORDER BY device OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY) b " \
                    f"ON a.device=b.device AND a.time=b.time"
        else:
            # node_data = [device_id, lng, lat, speed, time]
            query = f"SELECT node_id FROM {self._table} WHERE device={node_data[0]} AND lng={node_data[1]} " \
                    f"AND lat={node_data[2]} AND speed={node_data[3]}" \
                    f"AND time=(to_timestamp('1970/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS') " \
                    f"+ numtodsinterval({node_data[4]}, 'SECOND'))"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            if node_data is None:
                self.selected_data = cursor.fetchall()
            else:
                self.selected_data = cursor.fetchall()[0]
            return 0
        except Exception as e:
            if node_data is None:
                self._logger.error(f"Failed to select data from {self.dbms}. The error occurred: {e}")
            else:
                self._logger.error(f"Device: {node_data[0]}. Failed to select data from {self.dbms}. "
                                   f"The error occurred: {e}")
            self.selected_data = None
            return -11

    def close_connection(self):
        self.selected_data = None
        try:
            if self._connection.ping() is None:
                self._connection.close()
        except Exception:
            return


class ConnectionPostgresql(Connection):
    def __init__(self, logger):
        self.dbms = "PostgreSQL"
        super().__init__(logger)

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
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
            self._logger.error(f"Failed to connect to {self.dbms}. The error occurred: {e}")
            return -10

    def select_data(self, device_id):
        query = f"SELECT max(last_location_time) FROM {self._table} WHERE device_id={device_id};"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = cursor.fetchall()[0]
            return 0
        except Exception as e:
            self._logger.error(f"Device: {device_id}. Failed to select data from {self.dbms}. The error occurred: {e}")
            self.selected_data = None
            return -11

    def insert_data(self, values):
        query = f"INSERT INTO {self._table} VALUES(DEFAULT, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s, " \
                f"to_timestamp(%s), DEFAULT, %s)"
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, values)
        except Exception as e:
            self._logger.error(f"Device: {values[1]}. Failed to insert new row into geo_summary. "
                               f"The error occurred: {e}")
            self.selected_data = None
            return -11

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()
        self.selected_data = None


class ConnectionPostgis(Connection):
    def __init__(self, logger):
        self.dbms = "PostGIS"
        super().__init__(logger)

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
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
            self._logger.error(f"Failed to connect to {self.dbms}. The error occurred: {e}")
            return -10

    def select_data(self, lng, lat):
        # Buildings
        query = f"SELECT postcode, city, street, housenumber, name FROM osm_buildings " \
                f"WHERE ST_DWithin(Geography(ST_Transform(ST_Centroid(geometry), 4326)), " \
                f"Geography(ST_SetSRID(ST_Point({lng}, {lat}), 4326)), 100) AND street<>'' LIMIT 1;"
        if not self.execute_query(query):
            return 0

        # Roads
        query = f"SELECT network, ref, highway, name FROM osm_highway_linestring " \
                f"WHERE ST_Intersects(Geography(ST_Transform(geometry, 4326)), " \
                f"Geography(ST_SetSRID(ST_Point({lng}, {lat}), 4326))) IS TRUE AND name<>'' LIMIT 1;"
        if not self.execute_query(query):
            return 0

        # Water
        query = f"SELECT osm_water_polygon.natural, name FROM osm_water_polygon " \
                f"WHERE ST_Intersects(Geography(ST_Transform(geometry, 4326)), " \
                f"Geography(ST_SetSRID(ST_Point({lng}, {lat}), 4326))) IS TRUE AND name<>'' LIMIT 1;"
        if not self.execute_query(query):
            return 0

        # Cities
        query = f"SELECT postcode, country, region, district, type, name FROM osm_cities " \
                f"WHERE ST_Intersects(Geography(ST_Transform(geometry, 4326)), " \
                f"Geography(ST_SetSRID(ST_Point({lng}, {lat}), 4326))) IS TRUE AND name<>'' LIMIT 1;"
        if not self.execute_query(query):
            return 0

        # Boundaries
        query = f"SELECT type, name FROM osm_boundaries " \
                f"WHERE ST_Intersects(Geography(ST_Transform(geometry, 4326)), " \
                f"Geography(ST_SetSRID(ST_Point({lng}, {lat}), 4326))) IS TRUE AND name<>'' LIMIT 1;"
        if not self.execute_query(query):
            return 0
        else:
            self._logger.error(f"Failed to define device's address.")
            self.selected_data = None
            return -11

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()
        self.selected_data = None

    def execute_query(self, query):
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            cursor_data = cursor.fetchall()
            r = dict((cursor.description[i][0], value) for i, value in enumerate(cursor_data[0]))
            self.selected_data = json.dumps(r[0])
            return 0
        except Exception as e:
            return -11


class ConnectionRedis(Connection):
    def __init__(self, logger):
        self.dbms = "Redis"
        super().__init__(logger)

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
        try:
            self._connection = redis.Redis(
                host=self._host,
                port=self._port,
            )
            return 0
        except Exception as e:
            self._logger.error(f"Failed to connect to {self.dbms}. The error occurred: {e}")
            return -10

    def select_data(self, device_id):
        name = "device:" + str(device_id) + ":last_pos"
        try:
            data = self._connection.hmget(name, ["data"])
            if len(data) != 1:
                self._logger.error(f"Device: {device_id}. Failed to select data from {self.dbms}.")
                return -13

            gps_str = base64.b64decode(data[0].decode("utf-8"))
            gps = gps_data_pb2.GPS()
            gps.ParseFromString(gps_str)

            lng = gps.lon_deg + float(f"0.{gps.lon_flt}")
            lat = gps.lat_deg + float(f"0.{gps.lat_flt}")
            self.selected_data = [device_id, lng, lat, gps.speed, gps.ts]
            return 0
        except Exception as e:
            self._logger.error(f"Device: {device_id}. Failed to select data from {self.dbms}. The error occurred: {e}")
            self.selected_data = None
            return -11

    def close_connection(self):
        self.selected_data = None
        try:
            self._connection.ping()
            self._connection.close()
        except Exception:
            return


class ConnectionTimeZoneServer(Connection):
    def __init__(self, logger):
        self.dbms = "TimeZoneServer"
        super().__init__(logger)
        self._url = "http://" + self._host + ':' + str(self._port) + '/tz.json'

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
        self._connection = requests.Session()

    def select_data(self, lng, lat, ts_utc):
        data = {"lon": lng, "lat": lat, "t": ts_utc}
        try:
            response = self._connection.get(self._url, data=data)
            json_data = response.json()
            self.selected_data = int(json_data['shift']) / 3600
            return 0
        except Exception as e:
            self._logger.error(f"Failed to define timezone. The error occurred: {e}")
            self.selected_data = None
            return -11

    def close_connection(self):
        self.selected_data = None
        try:
            self._connection.close()
        except Exception:
            return

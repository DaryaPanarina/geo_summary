from abc import ABC, abstractmethod
import yaml
import base64
import json
from datetime import datetime

import mysql.connector
import cx_Oracle
import psycopg2
import redis
import requests

# Protobuf structure GPS
import proto_storage_pb2

class Connection(ABC):
    dbms = ""

    def __init__(self, config_file, logger):
        self._host = "-"
        self._port = "-"
        self._user = "-"
        self._password = "-"
        self._database = "-"
        self._table = "-"
        self._connection = None
        self.selected_data = None
        self._logger = logger
        with open(config_file, 'r') as stream:
            config = yaml.safe_load(stream)
        self._host = config[self.dbms]["host"]
        self._port = config[self.dbms]["port"]
        if "user" in config[self.dbms]:
            self._user = config[self.dbms]["user"]
        if "password" in config[self.dbms]:
            self._password = config[self.dbms]["password"]
        if "database" in config[self.dbms]:
            self._database = config[self.dbms]["database"]
        if "table" in config[self.dbms]:
            self._table = config[self.dbms]["table"]

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
    def __init__(self, config_file, logger):
        self.dbms = "MySQL"
        super().__init__(config_file, logger)
        if self._user == "-":
            raise Exception("'user'")
        if self._password == "-":
            raise Exception("'password'")
        if self._database == "-":
            raise Exception("'database'")
        if self._table == "-":
            raise Exception("'table'")
        self._cursor = None

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
            self._cursor = self._connection.cursor(prepared=True)
            return 0
        except Exception as e:
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, offset=-1, rows_number=10):
        if offset != -1:
            query = "SELECT device_id FROM {} ORDER BY device_id LIMIT %s, %s;".format(self._table)
        else:
            query = "SELECT count(device_id) FROM {};".format(self._table)
        try:
            if offset != -1:
                self._cursor.execute(query, (offset, rows_number))
                rows = self._cursor.fetchall()
                self.selected_data = [row[0] for row in rows]
            else:
                self._cursor.execute(query)
                self.selected_data = self._cursor.fetchall()[0][0]
            return 0
        except Exception as e:
            self._logger.error("Failed to select data from {}. The error occurred: {}.".format(self.dbms, e))
            self.selected_data = None
            return -11

    def close_connection(self):
        if (self._connection is not None) and self._connection.is_connected():
            self._connection.close()
        self.selected_data = None


class ConnectionOracle(Connection):
    def __init__(self, config_file, logger):
        self.dbms = "Oracle"
        super().__init__(config_file, logger)
        if self._user == "-":
            raise Exception("'user'")
        if self._password == "-":
            raise Exception("'password'")
        if self._database == "-":
            raise Exception("'database'")
        if self._table == "-":
            raise Exception("'table'")
        self._cursor = None
        self._cursor1 = None
        self._cursor2 = None

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

            self._cursor = self._connection.cursor()
            query = "SELECT time FROM {} WHERE device=:dev OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY".format(self._table)
            self._cursor.prepare(query)
            self._cursor1 = self._connection.cursor()
            query = "SELECT min(time) time FROM {} WHERE device=:dev".format(self._table)
            self._cursor1.prepare(query)
            self._cursor2 = self._connection.cursor()
            query = "SELECT lng, lat, speed FROM {} WHERE device=:dev AND " \
                    "time=TO_TIMESTAMP(:tm, 'DD-MM-YYYY HH24.MI.SS.FF') AND ROWNUM < 2".format(self._table)
            self._cursor2.prepare(query)
            return 0
        except Exception as e:
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, device):
        try:
            self._cursor.execute(None, dev=device)
            dev_time = self._cursor.fetchall()
            if len(dev_time) == 0:
                self._logger.error("Device: {}. Failed to select data from {}.".format(device, self.dbms))
                return -11
            self._cursor1.execute(None, dev=device)
            dev_time = self._cursor1.fetchall()[0][0]
            if dev_time is None:
                self._logger.error("Device: {}. Failed to select data from {}.".format(device, self.dbms))
                return -11
            self._cursor2.execute(None, dev=device, tm=dev_time.strftime('%d-%m-%Y %H.%M.%S.%f'))
            dev_data = self._cursor2.fetchall()[0]
            if dev_data[2] is None:
                self.selected_data = (dev_data[0], dev_data[1], 0, datetime.timestamp(dev_time))
            else:
                self.selected_data = (dev_data[0], dev_data[1], dev_data[2], datetime.timestamp(dev_time))
            return 0
        except Exception as e:
            self._logger.error("Device: {}. Failed to select data from {}. The error occurred: {}.".format(device,
                                                                                                           self.dbms,
                                                                                                           e))
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
    def __init__(self, config_file, logger):
        self.dbms = "PostgreSQL"
        super().__init__(config_file, logger)
        if self._user == "-":
            raise Exception("'user'")
        if self._password == "-":
            raise Exception("'password'")
        if self._database == "-":
            raise Exception("'database'")
        if self._table == "-":
            raise Exception("'table'")

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
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, device_ids):
        query = "SELECT DISTINCT ON (device_id) device_id, ST_X(last_location) lng, ST_Y(last_location) lat, " \
                "cast(extract(epoch FROM last_location_time) as bigint) last_location_time " \
                "FROM {} WHERE device_id IN ({}) " \
                "ORDER BY device_id, check_time DESC;".format(self._table, ",".join(map(str, device_ids)))
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            self.selected_data = dict((row[0], row) for row in rows)
            cursor.close()
            return 0
        except Exception as e:
            self._logger.error("Devices: {}. Failed to select data from {}. The error occurred: {}.".format(device_ids,
                                                                                                            self.dbms,
                                                                                                            e))
            self.selected_data = None
            return -11

    def insert_data(self, values):
        # values = [device_id, lng, lat, address, speed, last_location_time, timezone_shift]
        query = "INSERT INTO {} VALUES(DEFAULT, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s, " \
                "to_timestamp(%s) AT TIME ZONE 'UTC', DEFAULT, %s)".format(self._table)
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, values)
            cursor.close()
            return 0
        except Exception as e:
            self._logger.error("Device: {}. Failed to insert new row into geo_summary. "
                               "The error occurred: {}.".format(values[0], e))
            return -11

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()
        self.selected_data = None


class ConnectionOSM(Connection):
    def __init__(self, config_file, logger):
        self.dbms = "OSM"
        super().__init__(config_file, logger)
        if self._user == "-":
            raise Exception("'user'")
        if self._password == "-":
            raise Exception("'password'")
        if self._database == "-":
            raise Exception("'database'")

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
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, lng, lat):
        # Buildings
        query = "SELECT postcode, city, street, housenumber FROM osm_building_polygon " \
                "WHERE ST_DWithin(Geography(ST_Transform(ST_Centroid(geometry), 4326)), " \
                "Geography(ST_SetSRID(ST_Point({}, {}), 4326)), 100) AND street<>'' LIMIT 1;".format(lng, lat)

        error = self.execute_query(query)
        if (not error) and (self.selected_data['city']):
            self.selected_data = json.dumps(self.selected_data, sort_keys=True)
            return 0
        address = None
        if (not error) and (not self.selected_data['city']):
            address = self.selected_data

        # Cities
        query = "SELECT postcode, country, region, district, type, name as city FROM osm_cities " \
                "WHERE ST_Within(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry) " \
                "AND name<>'' LIMIT 1;".format(lng, lat)

        error = self.execute_query(query)
        if (not error) and not (address is None):
            address['city'] = self.selected_data['city']
            if not address['postcode']:
                address['postcode'] = self.selected_data['postcode']
            self.selected_data = json.dumps(address, sort_keys=True)
            return 0
        if error and not (address is None):
            self.selected_data = json.dumps(address, sort_keys=True)
            return 0
        if not error:
            address = self.selected_data

        # Roads
        query = "SELECT network, ref, highway, name FROM osm_highway_linestring " \
                "WHERE ST_DWithin(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry, 100) " \
                "AND name<>'' LIMIT 1;".format(lng, lat)
        if not self.execute_query(query):
            if not (address is None):
                self.selected_data['city'] = address['city']
            self.selected_data = json.dumps(self.selected_data, sort_keys=True)
            return 0

        # Water
        query = "SELECT osm_water_polygon.natural, name FROM osm_water_polygon " \
                "WHERE ST_Within(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry) " \
                "AND name<>'' LIMIT 1;".format(lng, lat)
        if not self.execute_query(query):
            if not (address is None):
                self.selected_data['city'] = address['city']
            self.selected_data = json.dumps(self.selected_data, sort_keys=True)
            return 0

        if not (address is None):
            self.selected_data = json.dumps(address, sort_keys=True)
            return 0

        # Nearest city
        query = "SELECT country, type, name as nearest_city FROM osm_cities WHERE name<>'' " \
                "ORDER BY ST_Distance(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry) " \
                "* COSD({}) LIMIT 1;".format(lng, lat, lat)

        if not self.execute_query(query):
            self.selected_data = json.dumps(self.selected_data, sort_keys=True)
            return 0
        else:
            self._logger.error("Lng: {}, lat: {}. Failed to define device's address.".format(lng, lat))
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
            if (len(cursor_data) != 1) or (cursor_data[0] is None):
                cursor.close()
                self.selected_data = None
                return -11
            self.selected_data = dict((cursor.description[i][0], value) for i, value in enumerate(cursor_data[0]))
            cursor.close()
            return 0
        except Exception as e:
            cursor.close()
            self._logger.error("The error occurred while defining device's address: {}".format(e))
            self.selected_data = None
            return -11


class ConnectionRedis(Connection):
    def __init__(self, config_file, logger):
        self.dbms = "Redis"
        super().__init__(config_file, logger)

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
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, device_id):
        name = "device:" + str(device_id) + ":info"
        try:
            ans = self._connection.get(name)
            if ans is None:
                self._logger.error("Device: {}. Failed to select data from {}.".format(device_id, self.dbms))
                return -13

            data_str = base64.b64decode(ans.decode("utf-8"))
            data = proto_storage_pb2.Data()
            data.ParseFromString(data_str)
            pos = data.position

            # [device_id, lng, lat, speed, ts]
            self.selected_data = [device_id, pos.x, pos.y, pos.s, pos.ts]
            return 0
        except Exception as e:
            self._logger.error("Device: {}. Failed to select data from {}. The error occurred: {}.".format(device_id,
                                                                                                           self.dbms, e))
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
    def __init__(self, config_file, logger):
        self.dbms = "TimeZoneServer"
        super().__init__(config_file, logger)
        self._url = "http://" + self._host + ':' + str(self._port) + '/tz.json'

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
        try:
            self._connection = requests.Session()
        except Exception as e:
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, lng, lat, ts_utc):
        data = {"lon": lng, "lat": lat, "t": ts_utc}
        try:
            response = self._connection.get(self._url, data=data)
            json_data = response.json()
            if 'failed' in json_data:
                self._logger.error("Failed to define timezone. Lat: {}, lng: {}, ts_utc: {}.".format(lat, lng, ts_utc))
                return -13
            self.selected_data = int(json_data['shift']) / 3600
            return 0
        except Exception as e:
            self._logger.error("Failed to define timezone. Lat: {}, lng: {}, ts_utc: {}. "
                               "The error occurred: {}.".format(lat, lng, ts_utc, e))
            self.selected_data = None
            return -11

    def close_connection(self):
        self.selected_data = None
        try:
            self._connection.close()
        except Exception:
            return

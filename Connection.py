from abc import ABC, abstractmethod
import yaml
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
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self):
        query = "SELECT device_id FROM {};".format(self._table)
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = cursor.fetchall()
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
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self):
        query = "SELECT DISTINCT a.device, a.lng, a.lat, a.speed, a.time FROM {} a " \
                "INNER JOIN (SELECT device, min(time) time FROM {} GROUP BY device ORDER BY device) b " \
                "ON a.device=b.device AND a.time=b.time".format(self._table, self._table)
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = cursor.fetchall()
            return 0
        except Exception as e:
            self._logger.error("Failed to select data from {}. The error occurred: {}.".format(self.dbms, e))
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

    def select_data(self, device_id):
        query = "SELECT ST_X(last_location) lng, ST_Y(last_location) lat, " \
                "cast(extract(epoch FROM last_location_time) as bigint) last_location_time " \
                "FROM {} WHERE device_id={} AND " \
                "check_time=(SELECT max(check_time) FROM {} WHERE device_id={});".format(self._table, device_id,
                                                                                         self._table, device_id)
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = cursor.fetchall()
            return 0
        except Exception as e:
            self._logger.error("Device: {}. Failed to select data from {}. The error occurred: {}.".format(device_id,
                                                                                                           self.dbms,
                                                                                                           e))
            self.selected_data = None
            return -11

    def insert_data(self, values):
        # values = [device_id, lng, lat, address, speed, last_location_time, timezone_shift]
        query = "INSERT INTO {} VALUES(DEFAULT, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s, to_timestamp(%s), " \
                "DEFAULT, %s)".format(self._table)
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, values)
            return 0
        except Exception as e:
            self._logger.error("Device: {}. Failed to insert new row into geo_summary. "
                               "The error occurred: {}.".format(values[0], e))
            return -11

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()
        self.selected_data = None


class ConnectionPostgis(Connection):
    def __init__(self, config_file, logger):
        self.dbms = "PostGIS"
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
            return 0
        except Exception as e:
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, lng, lat):
        # Buildings
        query = "SELECT postcode, city, street, housenumber, name FROM osm_buildings " \
                "WHERE ST_DWithin(Geography(ST_Transform(ST_Centroid(geometry), 4326)), " \
                "Geography(ST_SetSRID(ST_Point({}, {}), 4326)), 100) AND street<>'' LIMIT 1;".format(lng, lat)

        error = self.execute_query(query)
        if (not error) and (self.selected_data['city']):
            self.selected_data = json.dumps(self.selected_data)
            return 0
        address = None
        if (not error) and (not self.selected_data['city']):
            address = self.selected_data

        # Cities
        query = "SELECT postcode, country, region, district, type, name FROM osm_cities " \
                "WHERE ST_Within(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry) " \
                "AND name<>'' LIMIT 1;".format(lng, lat)

        error = self.execute_query(query)
        if (not error) and not (address is None):
            address['city'] = self.selected_data['name']
            if not address['postcode']:
                address['postcode'] = self.selected_data['postcode']
            self.selected_data = json.dumps(address)
            return 0
        if error and not (address is None):
            self.selected_data = json.dumps(address)
            return 0
        if not error:
            address = self.selected_data

        # Roads
        query = "SELECT network, ref, highway, name FROM osm_highway_linestring " \
                "WHERE ST_DWithin(Geography(ST_Transform(geometry, 4326)), " \
                "Geography(ST_SetSRID(ST_Point({}, {}), 4326)), 100) AND name<>'' LIMIT 1;".format(lng, lat)
        if not self.execute_query(query):
            if not (address is None):
                self.selected_data['city'] = address['name']
            self.selected_data = json.dumps(self.selected_data)
            return 0

        # Water
        query = "SELECT osm_water_polygon.natural, name FROM osm_water_polygon " \
                "WHERE ST_Within(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry) " \
                "AND name<>'' LIMIT 1;".format(lng, lat)
        if not self.execute_query(query):
            if not (address is None):
                self.selected_data['city'] = address['name']
            self.selected_data = json.dumps(self.selected_data)
            return 0

        if not (address is None):
            self.selected_data = json.dumps(address)
            return 0

        # Boundaries
        query = "SELECT type, name FROM osm_boundaries " \
                "WHERE ST_Within(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT({} {})'), 3857), geometry) " \
                "AND name<>'' LIMIT 1;".format(lng, lat)
        if not self.execute_query(query):
            self.selected_data = json.dumps(self.selected_data)
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
            self.selected_data = dict((cursor.description[i][0], value) for i, value in enumerate(cursor_data[0]))
            return 0
        except Exception:
            self.selected_data = None
            return -11


class ConnectionRedis(Connection):
    def __init__(self, config_file, logger):
        self.dbms = "Redis"
        super().__init__(config_file, logger)

    def __del__(self):
        self.close_connection()

    def create_connection(self, host):
        if host is None:
            host = self._host
            self.close_connection()
            self._connection = {}
        try:
            connection = redis.Redis(
                host=host,
                port=self._port,
            )
            self._connection[host] = connection
            return 0
        except Exception as e:
            self._logger.error("Failed to connect to {}. The error occurred: {}.".format(self.dbms, e))
            return -10

    def select_data(self, device_id):
        # Get IP of database where the device last position is stored
        name = "device:" + str(device_id) + ":connection_info"
        try:
            redis_ip = self._connection[self._host].hmget(name, "redis_ip")
            if (len(redis_ip) != 1) or (redis_ip[0] is None):
                self._logger.error("Device: {}. Failed to select data from {}.".format(device_id, self.dbms))
                return -13
        except Exception as e:
            self._logger.error("Device: {}. Failed to select data from {}. The error occurred: {}.".format(
                device_id, self.dbms, e))
            self.selected_data = None
            return -11

        # Get or create new connection to Redis
        redis_ip = str(redis_ip[0], 'utf-8')
        if not (redis_ip in self._connection):
            error = self.create_connection(redis_ip)
            if error:
                return error

        # Get last position of the device
        name = "device:" + str(device_id) + ":last_pos"
        try:
            data = self._connection[redis_ip].hmget(name, ["data"])
            if (len(data) != 1) or (data[0] is None):
                self._logger.error("Device: {}. Failed to select data from {}.".format(device_id, self.dbms))
                return -13

            gps_str = base64.b64decode(data[0].decode("utf-8"))
            gps = gps_data_pb2.GPS()
            gps.ParseFromString(gps_str)

            lng = gps.lon_deg + float("0.{}".format(abs(gps.lon_flt)))
            lat = gps.lat_deg + float("0.{}".format(abs(gps.lat_flt)))
            self.selected_data = [device_id, lng, lat, gps.speed, gps.ts]
            return 0
        except Exception as e:
            self._logger.error("Device: {}. Failed to select data from {}. The error occurred: {}.".format(
                device_id, self.dbms, e))
            self.selected_data = None
            return -11

    def close_connection(self):
        self.selected_data = None
        try:
            for i in self._connection.values():
                i.ping()
                i.close()
            self._connection.clear()
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
                self._logger.error("Failed to define timezone. Lat: {}, lng: {}, ts_utc: {}.").format(lat, lng, ts_utc)
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

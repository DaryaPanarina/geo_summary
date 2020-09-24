from abc import ABC, abstractmethod
import configparser
import mysql.connector
import cx_Oracle
import psycopg2


class Connection(ABC):
    dbms = ""

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self._host = config[self.dbms]["host"]
        self._port = config[self.dbms]["port"]
        self._user = config[self.dbms]["user"]
        self._password = config[self.dbms]["password"]
        self._database = config[self.dbms]["database"]
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
        query = f"SELECT * FROM {self._table} LIMIT 10;"
        if (self._connection is None) or (not self._connection.is_connected()):
            return -11
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -12

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
            self.__connection = cx_Oracle.connect(
                user=self._user,
                password=self._password,
                dsn=dsn_tns
            )
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -10

    def select_data(self):
        query = f"SELECT * FROM {self._table} WHERE ROWNUM < 10"
        if not self.is_open():
            return -11
        try:
            print(query)
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -12

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

    def select_data(self):
        query = f"SELECT * FROM {self._table} LIMIT 10;"
        if (self._connection is None) or self._connection.closed:
            return -11
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Exception as e:
            print("The error occurred: ", e)
            return -12

    def insert_data(self, values):
        query = f"INSERT INTO {self._table} VALUES(DEFAULT, %s, %s, POINT(%s, %s), %s, %s, TIMESTAMP %s, DEFAULT, %s)"
        if (self._connection is None) or self._connection.closed:
            return -11
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, values)
        except Exception as e:
            print("The error occurred: ", e)
            return -12

    def close_connection(self):
        if (self._connection is not None) and (not self._connection.closed):
            self._connection.close()

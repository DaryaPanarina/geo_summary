from abc import ABC, abstractmethod
import configparser
import mysql.connector
from mysql.connector import Error
from psycopg2 import OperationalError
import psycopg2


class Connection(ABC):
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
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.__host = config["MySQL"]["host"]
        self.__port = config["MySQL"]["port"]
        self.__user = config["MySQL"]["user"]
        self.__password = config["MySQL"]["password"]
        self.__database = config["MySQL"]["database"]
        self.__table = config["MySQL"]["table"]
        self.__connection = None
        self.selected_data = None

    def __del__(self):
        if (self.__connection is not None) and self.__connection.is_connected():
            self.__connection.close()

    def create_connection(self):
        try:
            self.__connection = mysql.connector.connect(
                host=self.__host,
                port=self.__port,
                user=self.__user,
                passwd=self.__password,
                database=self.__database
            )
            return 0
        except Error as e:
            print(f"The error '{e}' occurred")
            return -10

    def select_data(self):
        query = f"SELECT * FROM {self.__table} LIMIT 10;"
        if (self.__connection is None) or (not self.__connection.is_connected()):
            return -11
        try:
            cursor = self.__connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Error as e:
            print(f"The error '{e}' occurred")
            return -12

    def close_connection(self):
        if (self.__connection is not None) and self.__connection.is_connected():
            self.__connection.close()


class ConnectionPostgresql(Connection):
    def __init__(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.__host = config["PostgreSQL"]["host"]
        self.__port = config["PostgreSQL"]["port"]
        self.__user = config["PostgreSQL"]["user"]
        self.__password = config["PostgreSQL"]["password"]
        self.__database = config["PostgreSQL"]["database"]
        self.__table = config["PostgreSQL"]["table"]
        self.__connection = None
        self.selected_data = None

    def __del__(self):
        if (self.__connection is not None) and (not self.__connection.closed):
            self.__connection.close()

    def create_connection(self):
        try:
            self.__connection = psycopg2.connect(
                host=self.__host,
                port=self.__port,
                user=self.__user,
                password=self.__password,
                database=self.__database
            )
            self.__connection.autocommit = True
            return 0
        except OperationalError as e:
            print(f"The error '{e}' occurred")
            return -10

    def select_data(self):
        query = f"SELECT * FROM {self.__table} LIMIT 10;"
        if (self.__connection is None) or self.__connection.closed:
            return -11
        try:
            cursor = self.__connection.cursor()
            cursor.execute(query)
            self.selected_data = list(cursor.fetchall())
            return 0
        except Error as e:
            print(f"The error '{e}' occurred")
            return -12

    def insert_data(self, values):
        query = f"INSERT INTO {self.__table} VALUES(%s)"
        cursor = self.__connection.cursor()
        cursor.execute(query, values)

    def close_connection(self):
        if (self.__connection is not None) and (not self.__connection.closed):
            self.__connection.close()

#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import socket
import threading
import traceback
from queue import SimpleQueue
from logger import Logger


class Database(threading.Thread):

    def __init__(self):
        super().__init__()
        self.dbname = 'postgres'
        self.user = 'postgres'
        self.host = 'localhost'
        self.password = 'ridikelis'
        self.port = '5432'
        self.connection = None
        self.cursor = None
        self.dict_cursor = None
        self.running = False
        self.connected = False
        self.queue = SimpleQueue()
        self.logger = Logger('Database')
        self.lock = threading.Lock()

    def connect(self):
        self.logger.info(f"Trying to connect to {self.host}")
        args = f"dbname='{self.dbname}' user='{self.user}' host='{self.host}' password='{self.password}' port={self.port}"
        "'connect_timeout'=3 'keepalives'=1 'keepalives_idle'=5 'keepalives_interval'=2 'keepalives_count'=2"
        try:
            self.connection = psycopg2.connect(args)
            # This allows connection to raise psycopg2.OperationalError when database becomes unavailable
            # during transaction. Othervise, transaction hangs on cursor operations.
            # FOR UNIX LIKE MACHINES ONLY
            try:
                s = socket.fromfd(self.connection.fileno(), socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 6)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 2)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, 5000)
            except AttributeError:
                pass
            self.cursor = self.connection.cursor()
            self.dict_cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.connected = True
            self.logger.info(f"Successfully connected to database - {self.dbname}")
        except psycopg2.OperationalError as e:
            self.connected = False
            # Periodically try to reconnect.
            if self.running:
                threading.Timer(10, self.connect).start()
            self.logger.error(f"Unable to connect to database - {e}")

    def disconnect(self):
        if self.connected:
            self.cursor.close()
            self.connection.close()
            self.logger.warning(f"Disconnected from database - {self.dbname}")

    def insert_into(self, table, data):
        columns = data.keys()
        values = data.values()
        insert_que = f"INSERT INTO {table} (%s) VALUES %s"
        with self.lock:
            try:
                self.cursor.execute(insert_que, (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))
                self.connection.commit()
            except (psycopg2.OperationalError, TypeError) as e:
                self.logger.error(f"Unable to add data to database - {traceback.format_exc()}")
                self.cursor.execute("ROLLBACK")
                self.connection.commit()
            except psycopg2.errors.UniqueViolation as e:
                self.logger.warning(f"Data already exists in a database and will not be inserted.")
                self.cursor.execute("ROLLBACK")
                self.connection.commit()
            except (psycopg2.errors.NumericValueOutOfRange, psycopg2.errors.ForeignKeyViolation) as e:
                self.logger.error(f"Unable to add data to database - {traceback.format_exc()}")
                self.cursor.execute("ROLLBACK")
                self.connection.commit()

    def update_row(self, table, primary_key, data):
        req = f"UPDATE {table} SET "
        for k, v in data.items():
            if k != primary_key:
                if isinstance(v, int) or isinstance(v, float):
                    req += f"{k}={v},"
                else:
                    req += f"{k}='{v}',"
        req = req[:-1] + ' '
        # ADD LOCK MAYBE
        req += f"WHERE {primary_key}='{data[primary_key]}';"
        self.request(req, fetch=False)

    def request(self, req, fetch=True, dict_cursor=False):
        if self.connected:
            with self.lock:
                try:
                    if dict_cursor:
                        cursor = self.dict_cursor
                    else:
                        cursor = self.cursor
                    cursor.execute(req)
                    if fetch:
                        data = cursor.fetchall()
                        return data
                    else:
                        self.connection.commit()
                except psycopg2.OperationalError as e:
                    self.logger.error(f"Unable to execute the request - {traceback.format_exc()}")
                    self.cursor.execute("ROLLBACK")
                    self.connection.commit()
                except psycopg2.ProgrammingError as e:
                    self.logger.error(f"Unable to execute the request - {traceback.format_exc()}")
                    self.cursor.execute("ROLLBACK")
                    self.connection.commit()
                except psycopg2.errors.InFailedSqlTransaction as e:
                    self.logger.error(f"Unable to execute the request - {traceback.format_exc()}")
                    self.cursor.execute("ROLLBACK")
                    self.connection.commit()

    def stop(self):
        while not self.queue.empty():
            pass
        self.queue.put(None)
        self.running = False
        self.disconnect()

    def run(self):
        self.running = True
        self.connect()
        try:
            while self.running:
                table, data = self.queue.get()
                if data:
                    if table == 'job_listings':
                        if data.get('entered'):
                            self.insert_into(table, data)
                            self.logger.info(f"Data inserted for table {table}")
                        elif data.get('updated'):
                            self.update_row(table, 'url', data)
                    else:
                        self.insert_into(table, data)
                        self.logger.info(f"Data inserted for table {table}")
        except Exception:
            self.logger.error(f"UNKNOWN DATABASE ERROR OCCURRED:\n{traceback.format_exc()}\nDATA: {data}")

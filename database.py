#!/usr/bin/python3

import psycopg2
import socket
import threading
from queue import SimpleQueue


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
        self.running = False
        self.connected = False
        self.queue = SimpleQueue()

    def connect(self):
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
            self.connected = True
        except psycopg2.OperationalError:
            self.connected = False
            # Periodically try to reconnect.
            print('What?')
            if self.running:
                threading.Timer(10, self.connect).start()

    def disconnect(self):
        if self.connected:
            self.cursor.close()
            self.connection.close()

    def insert_into(self, table, data):
        columns = data.keys()
        values = data.values()
        insert_que = f"INSERT INTO {table} (%s) VALUES %s"
        try:
            self.cursor.execute(insert_que, (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))
            self.connection.commit()
        except (psycopg2.OperationalError, TypeError):
            print("Bibys")
            self.connected = False
            threading.Timer(10, self.connect).start()

    def request(self, req):
        if self.connected:
            try:
                self.cursor.execute(req)
                data = self.cursor.fetchall()
                return data
            except psycopg2.OperationalError:
                self.connected = False  

    def stop(self):
        while not self.queue.empty():
            pass
        self.queue.put(None)
        self.running = False
        self.disconnect()

    def run(self):
        self.running = True
        self.connect()
        while self.running:
            table, data = self.queue.get()
            if data:
                self.insert_into(table, data)

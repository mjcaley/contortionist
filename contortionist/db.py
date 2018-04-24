#!/usr/bin/env python3

import aiosqlite
import asyncio
import sqlite3
from enum import Enum
from collections import namedtuple
from functools import partial


class Query:
    def __init__(self, query, *parameters):
        self.query = query
        self.parameters = parameters
        self.results = None
        self.done = asyncio.Event()


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    # description read-only attribute
    # rowcount read only attribute

    async def close(self):
        '''Close cursor.'''
        pass

    async def execute(self, sql, parameters):
        pass


def connect(database, timeout=None, detect_types=None, isolation_level=None, cached_statements=None, uri=None, loop=None):
    loop = loop or asyncio.get_event_loop()
    connection = Connection(database, timeout, detect_types, isolation_level, cached_statements, uri, loop)
    connection.start()

    return connection


class Connection:
    def __init__(self, database, timeout=None, detect_types=None, isolation_level=None, cached_statements=None, uri=None, loop: asyncio.AbstractEventLoop=None):
        self.database = database
        self.timeout = timeout
        self.detect_types = detect_types
        self.isolation_level = isolation_level
        self.cached_statements = cached_statements
        self.uri = uri

        self._connection = None
        self._queries = asyncio.Queue()
        self.loop = loop

    def run(self):
        conn = sqlite3.connect(
            self.database,
            timeout=self.timeout,
            detect_types=self.detect_types,
            isolation_level=self.isolation_level,
            cached_statements=self.cached_statements,
            uri=self.uri
        )
        while True:
            query = asyncio.run_coroutine_threadsafe(self._queries.get(), self.loop)
            with self._connection.cursor() as cursor:
                try:
                    query.results = cursor.execute(query.query, *query.parameters)
                except Exception as e:
                    query.results = e
                finally:
                    self.loop.call_soon_threadsafe(query.done.set)

    async def start(self):
        self._connection = await self.loop.run_in_executor(None, self.run)

    def close(self):
        try:
            self._connection.cancel()
        except asyncio.CancelledError:
            print('SQLite connection cancelled')

# DB_NAME = 'contortionist.db'
#
# Message = namedtuple('Message', ('id', 'message', 'status'))
#
#
# class MessageStatus(Enum):
#     NEW = 1
#     WORKING = 2
#     SENDING = 3
#     DONE = 4
#     ERROR = 5
#
#
# class JobStatus(Enum):
#     NEW = 1
#     WORKING = 2
#     DONE = 3
#     ERROR = 4
#
#
# class TaskStatus(Enum):
#     NEW = 1
#     WORKING = 2
#     DONE = 3
#     ERROR = 4
#
#
# class CommandType(Enum):
#     Query = 1
#     Stop = 99
#
#
# class Command:
#     def __init__(self, command_type, *args):
#         self.command_type = command_type
#         self.args = args
#         self.result = None
#         self.finished = asyncio.Event()
#
#     def finish(self, result):
#         self.result = result
#         self.finished.set()
#
# class Connection:
#     def __init__(self, filename, loop=None):
#         self.filename = filename
#         self._connected = False
#         self._queue = asyncio.Queue()
#         self._loop = loop or asyncio.get_event_loop()
#
#     def run(self):
#         self._connected = True
#         self._loop.run_in_executor(None, self._run)
#
#     def _run(self):
#         # Connect to sqlite
#         while True:
#             future_command = asyncio.run_coroutine_threadsafe(self._queue.get())
#             command = future_command.result()
#             if command.command_type == CommandType.Stop:
#                 return
#             elif command.command_type == CommandType.Query:
#                 pass
#                 # run sql query
#                 # call done(result)
#
#     def stop(self):
#         if self._connected:
#             asyncio.run_coroutine_threadsafe(self._queue.put(Command.Stop), self._loop)
#             self._connected = False
#
#
# class Database:
#     def __init__(self, filename):
#         self.filename = filename
#
#     async def create_message(self, message, status=MessageStatus.NEW):
#         async with aiosqlite.connect(self.filename) as db:
#             result = await db.execute('INSERT INTO messages (message, status) VALUES (?, ?)',
#                                       (message, status))
#             await db.commit()
#
#             return result.lastrowid
#
#     async def get_message(self, message_id):
#         async with aiosqlite.connect(self.filename) as db:
#             cursor = await db.execute('SELECT id, message, status FROM messages WHERE id=?', (message_id,))
#             results = await cursor.fetchone()
#             return Message(id=results[0], message=results[1], status=MessageStatus(results[2]))
#
#     # async def message_status(self, message_id):
#     #     async with aiosqlite.connect(self.filename) as db:
#     #         result = await db.execute('SELECT (status) FROM messages WHERE id=?', (message_id,))
#     #         return MessageStatus((await result.fetchone())[0])
#
#     async def set_message_status(self, message_id, status):
#         async with aiosqlite.connect(self.filename) as db:
#             await db.execute('UPDATE messages SET status = ? WHERE id = ?', (message_id, status.value))
#
#     async def create_job(self, message_id, status=JobStatus.NEW):
#         async with aiosqlite.connect(self.filename) as db:
#             result = await db.execute('INSERT INTO jobs (message_id, status) VALUES (?, ?)',
#                                       (message_id, status.value))
#             await db.commit()
#
#             return result.lastrowid
#
#     async def job_status(self, job_id):
#         async with aiosqlite.connect(self.filename) as db:
#             result = await db.execute('SELECT (status) FROM jobs WHERE id=?', (job_id,))
#             return JobStatus((await result.fetchone())[0])
#
#     async def create_task(self, job_id, name, priority, status=TaskStatus.NEW):
#         async with aiosqlite.connect(self.filename) as db:
#             result = await db.execute('INSERT INTO tasks (job_id, name, priority, status) VALUES (?, ?, ?, ?)',
#                                       (job_id, name, priority, status.value))
#             await db.commit()
#
#             return result.lastrowid
#
#     async def task_status(self, task_id):
#         async with aiosqlite.connect(self.filename) as db:
#             result = await db.execute('SELECT (status) FROM tasks WHERE id=?', (task_id,))
#             return TaskStatus((await result.fetchone())[0])
#
#
# class Column:
#     def __init__(self, native_type, primary_key=False):
#         self.native_type = native_type
#         self.primary_key = primary_key
#
# class String(Column):
#     def __init__(self):
#
#
# class Session:
#     pass
#
#
# class MessageORM:
#     def __init__(self):
#         self._id = None
#         self._message = None
#         self._status = None
#
#     @property
#     def id(self):
#         return self._id
#
#     @id.setter
#     def id(self, value):
#         self._id = value
#
#     @property
#     def message(self):
#         return self._message
#
#     @message.setter
#     def message(self, value):
#         self._message = value
#
#     @property
#     def status(self):
#         return self._status
#
#     @status.setter
#     def status(self, value):
#         self._status = value

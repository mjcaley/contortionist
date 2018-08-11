#!/usr/bin/env python3

import asyncio

from collections import namedtuple
from enum import Enum

import aiosqlite


DB_NAME = 'contortionist.db'

Message = namedtuple('Message', ('id', 'message', 'status'))


class MessageStatus(Enum):
    NEW = 1
    WORKING = 2
    SENDING = 3
    DONE = 4
    ERROR = 5


class JobStatus(Enum):
    NEW = 1
    WORKING = 2
    DONE = 3
    ERROR = 4


class TaskStatus(Enum):
    NEW = 1
    WORKING = 2
    DONE = 3
    ERROR = 4


class CommandType(Enum):
    Query = 1
    Stop = 99


class Database:
    def __init__(self, database):
        self.database = database

    async def create_message(self, message, status=MessageStatus.NEW):
        async with aiosqlite.connect(self.database) as db:
            result = await db.execute('INSERT INTO messages (message, status) VALUES (?, ?)',
                                      (message, status))
            await db.commit()
            print(result)
        return result.lastrowid

    async def get_message(self, message_id):
        async with aiosqlite.connect(self.database) as db:
            cursor = await db.execute('SELECT id, message, status FROM messages WHERE id=?', (message_id,))
            results = await cursor.fetchone()
        return Message(id=results[0], message=results[1], status=MessageStatus(results[2]))

    # async def message_status(self, message_id):
    #     async with self.connection as db:
    #         result = await db.execute('SELECT (status) FROM messages WHERE id=?', (message_id,))
    #         return MessageStatus((await result.fetchone())[0])

    async def set_message_status(self, message_id, status):
        async with aiosqlite.connect(self.database) as db:
            await db.execute('UPDATE messages SET status = ? WHERE id = ?', (message_id, status.value))
            await db.commit()

    async def create_job(self, message_id, status=JobStatus.NEW):
        async with aiosqlite.connect(self.database) as db:
            result = await db.execute('INSERT INTO jobs (message_id, status) VALUES (?, ?)',
                                      (message_id, status.value))
            await db.commit()

            return result.lastrowid

    async def job_status(self, job_id):
        async with aiosqlite.connect(self.database) as db:
            result = await db.execute('SELECT (status) FROM jobs WHERE id=?', (job_id,))
            return JobStatus((await result.fetchone())[0])

    async def create_task(self, job_id, name, priority, status=TaskStatus.NEW):
        async with aiosqlite.connect(self.database) as db:
            result = await db.execute('INSERT INTO tasks (job_id, name, priority, status) VALUES (?, ?, ?, ?)',
                                      (job_id, name, priority, status.value))
            await db.commit()

            return result.lastrowid

    async def task_status(self, task_id):
        async with aiosqlite.connect(self.database) as db:
            result = await db.execute('SELECT (status) FROM tasks WHERE id=?', (task_id,))
            return TaskStatus((await result.fetchone())[0])


if __name__ == '__main__':
    async def run():
        d = Database(':memory:')

        await d.create_message(1)

#         conn = sqlite3.connect('new_db.sqlite')
#         conn.executescript('''
#         -- Contortionist database schema
#
# -- Schema version
# CREATE TABLE IF NOT EXISTS `meta` ( `schema` TEXT UNIQUE, `version` INTEGER, PRIMARY KEY(`schema`) );
# INSERT OR REPLACE INTO meta (schema, version) VALUES ('contortionist', 1);
#
# CREATE TABLE IF NOT EXISTS `messages` ( `id` INTEGER PRIMARY KEY AUTOINCREMENT,
#                                         `message` TEXT, `status` INTEGER NOT NULL DEFAULT 1 );
#
# CREATE TABLE IF NOT EXISTS `jobs` ( `id` INTEGER PRIMARY KEY AUTOINCREMENT,
#                                     `message_id` INTEGER NOT NULL,
#                                     `status` INTEGER DEFAULT 1,
#                                      FOREIGN KEY (`message_id`) REFERENCES `message(id)` );
#
# CREATE TABLE IF NOT EXISTS `tasks` ( `id` INTEGER PRIMARY KEY AUTOINCREMENT,
#                                      `job_id` INTEGER NOT NULL,
#                                      `name` TEXT NOT NULL,
#                                      `status` INTEGER DEFAULT 1,
#                                      `priority` INTEGER NOT NULL,
#                                       FOREIGN KEY (`job_id`) REFERENCES `jobs(id)` );
# ''')
#         conn.commit()
#         conn.execute('insert into messages values (1, 2, 3)')
#         conn.commit()
#         print(conn.execute('select * from messages').fetchall())


        # print(await d.create_message('this is a message'))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


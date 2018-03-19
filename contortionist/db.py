#!/usr/bin/env python3

import aiosqlite
from enum import Enum
from collections import namedtuple

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


class Database:
    def __init__(self, filename):
        self.filename = filename

    async def create_message(self, message, status=MessageStatus.NEW):
        async with aiosqlite.connect(self.filename) as db:
            result = await db.execute('INSERT INTO messages (message, status) VALUES (?, ?)',
                                      (message, status))
            await db.commit()

            return result.lastrowid

    async def get_message(self, message_id):
        async with aiosqlite.connect(self.filename) as db:
            cursor = await db.execute('SELECT id, message, status FROM messages WHERE id=?', (message_id,))
            results = await cursor.fetchone()
            return Message(id=results[0], message=results[1], status=MessageStatus(results[2]))

    # async def message_status(self, message_id):
    #     async with aiosqlite.connect(self.filename) as db:
    #         result = await db.execute('SELECT (status) FROM messages WHERE id=?', (message_id,))
    #         return MessageStatus((await result.fetchone())[0])

    async def set_message_status(self, message_id, status):
        async with aiosqlite.connect(self.filename) as db:
            await db.execute('UPDATE messages SET status = ? WHERE id = ?', (message_id, status.value))

    async def create_job(self, message_id, status=JobStatus.NEW):
        async with aiosqlite.connect(self.filename) as db:
            result = await db.execute('INSERT INTO jobs (message_id, status) VALUES (?, ?)',
                                      (message_id, status.value))
            await db.commit()

            return result.lastrowid

    async def job_status(self, job_id):
        async with aiosqlite.connect(self.filename) as db:
            result = await db.execute('SELECT (status) FROM jobs WHERE id=?', (job_id,))
            return JobStatus((await result.fetchone())[0])

    async def create_task(self, job_id, name, priority, status=TaskStatus.NEW):
        async with aiosqlite.connect(self.filename) as db:
            result = await db.execute('INSERT INTO tasks (job_id, name, priority, status) VALUES (?, ?, ?, ?)',
                                      (job_id, name, priority, status.value))
            await db.commit()

            return result.lastrowid

    async def task_status(self, task_id):
        async with aiosqlite.connect(self.filename) as db:
            result = await db.execute('SELECT (status) FROM tasks WHERE id=?', (task_id,))
            return TaskStatus((await result.fetchone())[0])

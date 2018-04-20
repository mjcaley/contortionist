import asyncio
import sqlite3

from asyncio import Event
from queue import Queue


class AsyncDbConnection:
    def __init__(self):
        pass

    def connect(self, *args):
        pass


class Query:
    def __init__(self, query):
        self.query = query
        self.result = None
        self.done = Event()

    async def wait(self):
        await self.done.wait()
        return self.result


class SqliteConnection:
    class Cancel: pass

    def __init__(self, filename, loop=None):
        self.filename = filename
        self._connection = None
        self._query_queue = Queue()
        self.loop = loop or asyncio.get_event_loop()
        self._task = None

    async def connect(self):
        pass

    async def cancel(self):
        print('injecting cancel')
        self._query_queue.put(self.Cancel())
        await self._task

    def _run(self):
        while True:
            print('waiting for query')
            query = self._query_queue.get()
            print('got item')
            if isinstance(query, self.Cancel):
                return
            print(query.query)
            query.result = 'a result'
            query.done.set()
            self._query_queue.task_done()

    def run(self):
        self._task = self.loop.run_in_executor(executor=None, func=self._run)

    async def execute(self, stmt):
        query = Query(stmt)
        self._query_queue.put(query)
        await query.done.wait()
        return query.result


async def make_query(db_conn):
    result = await db_conn.execute('SELECT * FROM TABLE;')
    print('got a result', result)


async def connect():
    conn = SqliteConnection(':memory:')
    conn.run()
    return conn


async def main():
    print('connect')
    conn = await connect()
    print('make_query')
    await make_query(conn)
    print('cancel')
    await conn.cancel()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    db_conn = loop.run_until_complete(main())
    # print('DB connection started')
    # loop.run_until_complete(make_query(db_conn))
    # print('quitting')
    # db_conn.cancel()

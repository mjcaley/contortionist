#!/usr/bin/env python3

import asyncio
from pathlib import Path
from uuid import uuid4

import aiofiles

from .exceptions import TaskAlreadyRunningException


class MailStoreController:
    def __init__(self, path, from_queue, to_queue, logger, *, loop=None):
        self._task = None
        self._run = False
        self.path = Path(path)
        self.from_queue = from_queue
        self.to_queue = to_queue
        self.logger = logger
        self.loop = loop or asyncio.get_event_loop()

    def _running(self):
        return self._run or not self.from_queue.empty()

    async def dequeue(self):
        while self._running():
            message = await self.from_queue.get()
            self.loop.create_task(self.store_message(message))

    @staticmethod
    def generate_filename():
        return str(uuid4())

    async def store_message(self, message):
        try:
            async with aiofiles.open(Path(self.path / self.generate_filename()), 'w') as f:
                await f.write(str(message))
                await self.to_queue.put(message)
        except IOError as e:
            self.logger.error('Error writing message to disk (Message ID: %s; Exception: %s)', message['Message-ID'], e)
        except PermissionError as e:
            self.logger.error('Permission error writing message to disk (Message ID: %s, Exception: %s)', message['Message-ID'], e)
        finally:
            self.from_queue.task_done()

    async def run(self):
        if not self._task:
            self._task = self.loop.create_task(self.dequeue())
        else:
            raise TaskAlreadyRunningException

    async def stop(self):
        self._run = False
        if self._task:
            await self._task
        self._task = None
        print('[TaskRunner] cancelled')

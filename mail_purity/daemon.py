#!/usr/bin/env python3

import asyncio
from asyncio import Queue
from email.message import EmailMessage
from functools import partial
import logging

import aiosmtpd
import aiosmtplib

from aiosmtpd.handlers import AsyncMessage
from aiosmtpd.smtp import SMTP


class TaskCommand:
    pass


class Kill(TaskCommand):
    pass


class TaskRunner:
    def __init__(self, *, loop=None):
        self._task = None
        self.loop = loop or asyncio.get_event_loop()

    def run(self):
        if not self._task:
            self._task = self.loop.create_task(self._run())
        else:
            raise Exception

    async def _stop(self):
        pass

    async def stop(self):
        await self._stop()
        self._task.cancel()
        try:
            await self._task
            self._task = None
        except asyncio.CancelledError:
            print('[TaskRunner] cancelled')


class DebugQueueMover(TaskRunner):
    def __init__(self, from_queue: Queue, to_queue: Queue, *, loop=None):
        self.from_queue = from_queue
        self.to_queue = to_queue
        super().__init__(loop=loop)

    async def _run(self):
        while True:
            await asyncio.sleep(1)
            print('[DebugQueueMover] getting item')
            item = await self.from_queue.get()
            print('[DebugQueueMover] putting item')
            await self.to_queue.put(item)
            self.from_queue.task_done()

    async def _stop(self):
        print('[DebugQueueMover] user stopping')
        await self.from_queue.join()
        print('[DebugQueueMover] user stopped')


class DebugQueueDumper(TaskRunner):
    def __init__(self, queue: Queue, *, loop=None):
        self.queue = queue
        super().__init__(loop=loop)

    async def _run(self):
        while True:
            await asyncio.sleep(2)
            print('[DebugQueueDumper] getting item')
            item = await self.queue.get()
            self.queue.task_done()

    async def _stop(self):
        print('[DebugQueueDumper] user stopping')
        await self.queue.join()
        print('[DebugQueueDumper] user stopped')


class Handler(AsyncMessage):
    def __init__(self, incoming_queue: Queue, message_class=None, *, loop=None):
        self.incoming_queue = incoming_queue
        super().__init__(message_class, loop=loop)

    async def handle_message(self, message):
        print('inside handle_DATA')
        await self.incoming_queue.put(message)
        print('inserted email')


class SMTPRunner:
    def __init__(self, incoming_queue, config, *, loop=None):
        self.incoming_queue = incoming_queue
        self.config = config
        self.smtp_server = None
        self.loop = loop or asyncio.get_event_loop()

    async def run(self):
        self.smtp_server = await self.loop.create_server(
            partial(SMTP, handler=Handler(self.incoming_queue, loop=self.loop), enable_SMTPUTF8=True),
            port=10025  # from config
        )  # Replace with manager class like the others

    async def stop(self):
        self.smtp_server.close()
        await self.smtp_server.wait_closed()
        self.smtp_server = None


class Client:
    def __init__(self, outgoing_queue: Queue, *, loop=None):
        self.outgoing_queue = outgoing_queue
        self._task: asyncio.Task = None
        self.loop = loop or asyncio.get_event_loop()

    async def _run(self):
        while True:
            message = await self.outgoing_queue.get()
            # establish connection if needed
            # send message

    def run(self):
        self._task = self.loop.create_task(self._run())

    async def stop(self):
        await self.outgoing_queue.join()
        self._task.cancel()


class MailSaver(TaskRunner):
    def __init__(self, incoming_queue: Queue, processing_queue: Queue, *, loop=None):
        self.incoming_queue = incoming_queue
        self.processing_queue = processing_queue
        super().__init__(loop=loop)


class Filter:
    def __init__(self, processing_queue: Queue, outgoing_queue: Queue, *, loop=None):
        pass


class Daemon:
    def __init__(self, config=None, *, loop=None):
        self.config = config

        self.incoming_mail = Queue()
        self.processing_mail = Queue()
        self.outgoing_mail = Queue()

        self.smtp_server: asyncio.AbstractServer = SMTPRunner(self.incoming_mail, None, loop=loop)
        self.mail_saver = DebugQueueMover(self.incoming_mail, self.processing_mail)
        self.filter_manager = DebugQueueMover(self.processing_mail, self.outgoing_mail)
        self.smtp_client = DebugQueueDumper(self.outgoing_mail)

        self.db_connection = None

        self.loop = loop or asyncio.get_event_loop()

    async def run(self):
        # check for a lock file
        await self.smtp_server.run()
        self.mail_saver.run()
        self.filter_manager.run()
        self.smtp_client.run()

        print('running the server apparently')

    async def stop(self):
        print('stopping server')
        await self.smtp_server.stop()
        print('SMTP server stopped')
        await self.mail_saver.stop()
        print('mail saver stopped')
        await self.filter_manager.stop()
        print('filter manager stopped')
        await self.smtp_client.stop()
        print('SMTP client stopped')


async def send(mail, loop):
    smtp = aiosmtplib.SMTP(hostname='127.0.0.1', port=10025, loop=loop)
    print('client connecting')
    await smtp.connect()
    print('client connected')
    await smtp.send_message(mail)


async def dump_queue(queue):
    print('dumping queue')
    while not queue.empty():
        item = await queue.get()
        print(item)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    # logging.basicConfig(level=logging.DEBUG)
    d = Daemon(loop=loop)
    loop.run_until_complete(d.run())
    msg = EmailMessage()
    msg['From'] = 'from@example.com'
    msg['To'] = 'to@example.com'
    msg['Subject'] = 'Example message'
    loop.run_until_complete(asyncio.gather(send(msg, loop), send(msg, loop), send(msg, loop), send(msg, loop), send(msg, loop)))
    loop.run_until_complete(d.stop())

    loop.run_until_complete(dump_queue(d.incoming_mail))

#!/usr/bin/env python3

from asyncio import Queue
from email.message import EmailMessage
import logging

import pytest

from contortionist.exceptions import TaskAlreadyRunningException
from contortionist.mail_store_controller import MailStoreController


def test_running_true_and_empty_queue(tmpdir):
    msc = MailStoreController(
        tmpdir,
        Queue(),
        Queue(),
        logging.getLogger()
    )
    msc._run = True

    assert msc._running() is True


def test_running_false_and_item_in_queue(tmpdir):
    msc = MailStoreController(
        tmpdir,
        Queue(),
        Queue(),
        logging.getLogger()
    )
    msc._run = False
    msc.from_queue.put_nowait(True)

    assert msc._running() is True


def test_running_false_and_empty_queue(tmpdir):
    msc = MailStoreController(
        tmpdir,
        Queue(),
        Queue(),
        logging.getLogger()
    )
    msc._run = False

    assert msc._running() is False


def test_generate_filename(tmpdir):
    msc = MailStoreController(
        tmpdir,
        Queue(),
        Queue(),
        logging.getLogger()
    )
    filename = msc.generate_filename()

    assert isinstance(filename, str)


@pytest.mark.asyncio
async def test_store_message(tmpdir):
    msc = MailStoreController(
        tmpdir,
        Queue(),
        Queue(),
        logging.getLogger()
    )
    email = EmailMessage()
    email['To'] = 'to@example.org'
    email['From'] = 'from@example.org'
    email['Subject'] = 'Subject'
    msc.from_queue.put_nowait(email)
    await msc.store_message(msc.from_queue.get_nowait())

    assert len(tmpdir.listdir()) == 1

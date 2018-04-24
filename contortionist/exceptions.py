#!/usr/bin/env python3


class DispatcherException(Exception):
    pass


class TaskAlreadyRunningException(DispatcherException):
    pass

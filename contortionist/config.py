#!/usr/bin/env python3

import toml


def load_configuration(file):
    return toml.load(file)

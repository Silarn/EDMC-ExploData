# -*- coding: utf-8 -*-
# ExploData module plugin for EDMC
# Source: https://github.com/Silarn/EDMC-ExploData
# Licensed under the [GNU Public License (GPL)](http://www.gnu.org/licenses/gpl-2.0.html) version 2 or later.

import tkinter as tk
from typing import Optional

import explo_data.const
from EDMCLogging import get_plugin_logger
from explo_data import db


class This:
    """Holds module globals."""

    def __init__(self):
        self.NAME: str = explo_data.const.plugin_name
        self.VERSION: str = explo_data.const.plugin_version


this = This()
logger = get_plugin_logger(this.NAME)


def plugin_start3(plugin_dir: str) -> str:
    db.init()
    return 'ExploData'


def plugin_app(parent: tk.Frame) -> Optional[tk.Frame]:
    return None


def plugin_stop():
    """
    EDMC plugin stop function. Closes open threads and database sessions for clean shutdown.
    """

    db.shutdown()

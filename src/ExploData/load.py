# -*- coding: utf-8 -*-
# ExploData module plugin for EDMC
# Source: https://github.com/Silarn/EDMC-ExploData
# Licensed under the [GNU Public License (GPL)](http://www.gnu.org/licenses/gpl-2.0.html) version 2 or later.

import tkinter as tk
from typing import Optional, Mapping, MutableMapping

from EDMCLogging import get_plugin_logger

import explo_data.const
from ExploData.explo_data.journal_parse import JournalParse, fire_event_callbacks
from explo_data import db


class This:
    """Holds module globals."""

    def __init__(self):
        self.NAME: str = explo_data.const.plugin_name
        self.VERSION: str = explo_data.const.plugin_version

        self.journal_processor: Optional[JournalParse] = None


this = This()
logger = get_plugin_logger(this.NAME)


def plugin_start3(plugin_dir: str) -> str:
    db.init()
    this.journal_processor = JournalParse(db.get_session())
    return 'ExploData'


def plugin_app(parent: tk.Frame) -> Optional[tk.Frame]:
    """
    EDMC plugin app hook. Builds TKinter display.

    :param parent: EDMC main frame.
    :return: None, as we have no display.
    """

    return None


def plugin_stop():
    """
    EDMC plugin stop function. Closes open threads and database sessions for clean shutdown.
    """

    db.shutdown()


def journal_entry(
        cmdr: str, is_beta: bool, system: str, station: str, entry: Mapping[str, any], state: MutableMapping[str, any]
) -> str:
    if not state['StarPos'] or not system or not cmdr:
        return ''

    this.journal_processor.set_cmdr(cmdr)
    this.journal_processor.set_system(system, state['StarPos'])
    this.journal_processor.process_entry(entry)
    fire_event_callbacks(entry)

    return ''

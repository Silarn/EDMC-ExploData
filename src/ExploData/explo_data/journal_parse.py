# -*- coding: utf-8 -*-
# ExploData module plugin for EDMC
# Source: https://github.com/Silarn/EDMC-ExploData
# Licensed under the [GNU Public License (GPL)](http://www.gnu.org/licenses/gpl-2.0.html) version 2 or later.

import concurrent
import json
import re
import threading
import tkinter as tk
from concurrent.futures import Future
from datetime import datetime
from os import listdir, cpu_count
from os.path import expanduser
from pathlib import Path
from time import sleep
from threading import Event
from typing import Any, BinaryIO, Callable, Mapping, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError as AlcIntegrityError
from sqlalchemy.orm import Session
from sqlite3 import IntegrityError

from EDMCLogging import get_plugin_logger
from config import config
from .RegionMap import findRegion

from ExploData.explo_data import const
from .bio_data.codex import parse_variant, set_codex
from .db import System, Commander, Planet, JournalLog, get_session, SystemStatus, PlanetStatus
from .body_data.struct import PlanetData, StarData, NonBodyData

JOURNAL_REGEX = re.compile(r'^Journal(Alpha|Beta)?\.[0-9]{2,4}-?[0-9]{2}-?[0-9]{2}T?[0-9]{2}[0-9]{2}[0-9]{2}'
                           r'\.[0-9]{2}\.log$')
JOURNAL1_REGEX = re.compile(r'^Journal(Alpha|Beta)?\.([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})'
                            r'\.([0-9]){2}\.log$')
JOURNAL2_REGEX = re.compile(r'^Journal(Alpha|Beta)?\.([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})'
                            r'\.([0-9]{2})\.log$')


class This:
    """Holds globals."""

    def __init__(self):
        self.migration_failed: bool = False
        self.journal_thread: Optional[threading.Thread] = None
        self.parsing_journals: bool = False
        self.journal_stop: bool = False
        self.journal_event: Optional[threading.Event] = None
        self.journal_progress: tuple[int, int] = (0, 0)
        self.journal_error: bool = False

        self.journal_processing_callbacks: dict[str, tk.Frame] = {}
        self.event_callbacks: dict[str, set[Callable]] = {}


this = This()
logger = get_plugin_logger(const.plugin_name)


class JournalParse:
    """
    This class is a general purpose container to process individual journal files. It's used both by the main
    EDMC journal parser hook and by the threaded journal import function, generally called by other plugins.
    """
    def __init__(self, session: Session):
        self._session: Session = session
        self._cmdr: Optional[Commander] = None
        self._system: Optional[System] = None

    def parse_journal(self, journal: Path, event: Event) -> int:
        """
        Function used to kick on a full journal import.

        :param journal: The journal file to parse
        :param event: The threaded Event used to interrupt the process
        :return: Success or failure of the journal import
        """
        if event.is_set():
            return True
        found = self._session.scalar(select(JournalLog).where(JournalLog.journal == journal.name))
        failures = 0
        if not found:
            log: BinaryIO = open(journal, 'rb', 0)
            for line in log:
                retry = 2
                while True:
                    result = self.parse_entry(line)
                    if not result:
                        failures += 1
                    if (failures >= 3 and retry == 0) or event.is_set():
                        return 1
                    elif result:
                        break
                    retry -= 1
                    sleep(.1)
        else:
            self._session.expunge(found)
            return 2

        journal = JournalLog(journal=journal.name)
        try:
            self._session.add(journal)
            self._session.commit()
        except (IntegrityError, AlcIntegrityError):
            self._session.expunge(journal)
        return 0

    def parse_entry(self, line: bytes) -> bool:
        """
        Parse a single line of a journal file. Load as JSON and pass to the processor.

        :param line: The line of the journal file
        :returns: False if an Exception occurs, otherwise true
        """
        if line is None:
            return False

        try:
            entry: Mapping[str, Any] = json.loads(line)
            self.process_entry(entry)
        except Exception as ex:
            logger.error(f'Invalid journal entry:\n{line!r}\n', exc_info=ex)
            return False
        return True

    def process_entry(self, entry: Mapping[str, Any]) -> None:
        """
        Main journal entry processor. Parses important events and submits the appropriate data objects to the database.

        :param entry: JSON object of the current journal line
        """
        event_type = entry['event'].lower()
        match event_type:
            case 'loadgame':
                self._session.close()
                self.set_cmdr(entry['Commander'])
            case 'commander' | 'newcommander':
                self._session.close()
                self.set_cmdr(entry['Name'])
            case 'location' | 'fsdjump' | 'carrierjump':
                self._session.close()
                self.set_system(entry['StarSystem'], entry.get('StarPos', None))
            case 'scan':
                if not self._system:
                    return
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr) if self._cmdr else None
                if 'StarType' in entry:
                    self.add_star(entry)
                elif 'PlanetClass' in entry and entry['PlanetClass']:
                    self.add_planet(entry)
                else:
                    non_body = NonBodyData.from_journal(self._system, self.get_body_name(entry['BodyName']),
                                                        entry['BodyID'], self._session)
                    if self._cmdr:
                        non_body.set_discovered(True, self._cmdr.id).set_was_discovered(entry['WasDiscovered'],
                                                                                        self._cmdr.id)

            case 'fssdiscoveryscan':
                if not self._system or not self._cmdr:
                    return
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr)
                status = self.get_system_status()
                status.honked = True
                self._system.body_count = entry['BodyCount']
                self._system.non_body_count = entry['NonBodyCount']
                if entry['Progress'] == 1.0:
                    status.fully_scanned = True
                self._session.commit()
            case 'fssbodysignals' | 'saasignalsfound':
                if self._system is None:
                    return
                self._system = self._session.merge(self._system)
                self.add_signals(entry)
            case 'fssallbodiesfound':
                if not self._system or not self._cmdr:
                    return
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr)
                status = self.get_system_status()
                status.fully_scanned = True
                self._session.commit()
            case 'saascancomplete':
                if not self._system or not self._cmdr:
                    return
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr)
                body_short_name = self.get_body_name(entry['BodyName'])
                if body_short_name.endswith('Ring') or body_short_name.find('Belt Cluster') != -1:
                    body: NonBodyData = NonBodyData.from_journal(self._system, body_short_name,
                                                                 entry['BodyID'], self._session)
                else:
                    body: PlanetData = PlanetData.from_journal(self._system, body_short_name,
                                                               entry['BodyID'], self._session)
                target = int(entry['EfficiencyTarget'])
                used = int(entry['ProbesUsed'])
                body.set_mapped(True, self._cmdr.id)\
                    .set_efficient(target >= used, self._cmdr.id)
                if self.get_system_status().fully_scanned:
                    count = 0
                    for planet in self._system.planets:
                        planet_status = self.get_planet_status(planet)
                        if planet_status.mapped:
                            count += 1
                    if len(self._system.planets) == count:
                        self.get_system_status().fully_mapped = True
                self._session.commit()
            case 'scanorganic':
                if not self._system or not self._cmdr:
                    return
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr) if self._cmdr else None
                self.add_scan(entry)
            case 'codexentry':
                if entry['Category'] == '$Codex_Category_Biology;' and 'BodyID' in entry:
                    if not self._system or not self._cmdr:
                        return
                    self._system = self._session.merge(self._system)
                    self._cmdr = self._session.merge(self._cmdr) if self._cmdr else None
                    planet: Planet = self._session.scalar(select(Planet).where(Planet.system_id == self._system.id)
                                                          .where(Planet.body_id == entry['BodyID']))
                    if not planet:
                        return

                    target_body = PlanetData(self._system, planet, self._session)

                    genus, species, color = parse_variant(entry['Name'])
                    if genus is not '' and species is not '':
                        target_body.add_flora(genus, species, color)

                    if self._cmdr:
                        set_codex(self._cmdr.id, entry['Name'], self._system.region)

    def get_body_name(self, fullname: str) -> str:
        """
        Remove the base system name from the body name if the body has a unique identifier.
        Usually only the main star has the same name as the system in one-star systems.

        :param fullname: The full name of the body including the system name
        :return: The short name of the body unless it matches the system name
        """
        self._system = self._session.merge(self._system)
        if fullname.startswith(self._system.name + ' '):
            body_name = fullname[len(self._system.name + ' '):]
        else:
            body_name = fullname
        return body_name

    def set_cmdr(self, name: str) -> None:
        """
        Submit or create a Commander entry and save it to the local journal processor
        """
        self._session.commit()
        self._session.close()

        self._cmdr = self._session.scalar(select(Commander).where(Commander.name == name))

        if not self._cmdr:
            self._cmdr = Commander(name=name)
            self._session.add(self._cmdr)
            self._session.commit()

    def set_system(self, name: str, address: list[float]) -> None:
        """
        Submit or create a System entry and save it to the local journal processor
        """
        if not address:
            return
        self._system = self._session.scalar(select(System).where(System.name == name))
        if not self._system:
            self._system = System(name=name)
            self._session.add(self._system)
        self._system.x = address[0]
        self._system.y = address[1]
        self._system.z = address[2]
        region = findRegion(self._system.x, self._system.y, self._system.z)
        if region:
            self._system.region = region[0]
        self._session.commit()

    def get_system_status(self) -> SystemStatus:
        """
        Fetch or create the SystemStatus data attached to the local System object
        """
        statuses: list[SystemStatus] = self._system.statuses
        statuses = list(filter(lambda item: item.commander_id == self._cmdr.id, statuses))
        if len(statuses):
            status = statuses[0]
        else:
            status = SystemStatus(system_id=self._system.id, commander_id=self._cmdr.id)
            self._system.statuses.append(status)
            self._session.commit()
        return status

    def get_planet_status(self, planet: Planet) -> PlanetStatus:
        """
        Fetch or create the SystemStatus data attached to the local System object
        """
        statuses: list[PlanetStatus] = planet.statuses
        statuses = list(filter(lambda item: item.commander_id == self._cmdr.id, statuses))
        if len(statuses):
            status = statuses[0]
        else:
            status = PlanetStatus(planet_id=planet.id, commander_id=self._cmdr.id)
            planet.statuses.append(status)
            self._session.commit()
        return status

    def add_star(self, entry: Mapping[str, Any]) -> None:
        """
        Add star data from journal event

        :param entry: The journal event dict (must be a Scan event with star data)
        """

        was_discovered = entry['ScanType'] == 'NavBeaconDetail' or entry['WasDiscovered']
        body_short_name = self.get_body_name(entry['BodyName'])
        star_data = StarData.from_journal(self._system, body_short_name, entry['BodyID'], self._session)

        star_data.set_distance(float(entry['DistanceFromArrivalLS'])).set_type(entry['StarType']) \
            .set_mass(entry['StellarMass']).set_subclass(entry['Subclass']).set_luminosity(entry['Luminosity']) \
            .set_rotation(entry['RotationPeriod']).set_orbital_period(entry.get('OrbitalPeriod', 0))
        if self._cmdr:
            star_data.set_discovered(True, self._cmdr.id).set_was_discovered(was_discovered, self._cmdr.id)

        if 'Rings' in entry:
            for ring in entry['Rings']:
                ring_name = ring['Name'][len(entry['BodyName'])+1:]
                star_data.add_ring(ring_name, ring['RingClass'])

    def add_planet(self, entry: Mapping[str, Any]) -> None:
        """
        Add planet data from journal event

        :param entry: The journal event dict (must be a Scan event with planet data)
        """

        was_discovered = entry['ScanType'] == 'NavBeaconDetail' or entry['WasDiscovered']
        scan_type = get_scan_type(entry['ScanType'])
        body_short_name = self.get_body_name(entry['BodyName'])
        body_data = PlanetData.from_journal(self._system, body_short_name, entry['BodyID'], self._session)
        body_data.set_distance(float(entry['DistanceFromArrivalLS'])).set_type(entry['PlanetClass']) \
            .set_mass(entry['MassEM']).set_gravity(entry['SurfaceGravity']) \
            .set_temp(entry.get('SurfaceTemperature', None)).set_pressure(entry.get('SurfacePressure', None)) \
            .set_radius(entry['Radius']).set_volcanism(entry.get('Volcanism', None)) \
            .set_rotation(entry['RotationPeriod']).set_orbital_period(entry.get('OrbitalPeriod', 0)) \
            .set_landable(entry.get('Landable', False)).set_terraform_state(entry.get('TerraformState', ''))

        if self._cmdr:
            body_data.set_discovered(True, self._cmdr.id).set_was_discovered(was_discovered, self._cmdr.id) \
                .set_was_mapped(entry['WasMapped'], self._cmdr.id).set_scan_state(scan_type, self._cmdr.id)

        star_search = re.search('^([A-Z]+) .+$', body_short_name)
        if star_search:
            for star in star_search.group(1):
                body_data.add_parent_star(star)
        else:
            body_data.add_parent_star(self._system.name)

        if 'Materials' in entry:
            for material in entry['Materials']:
                body_data.add_material(material['Name'])

        if 'AtmosphereType' in entry:
            body_data.set_atmosphere(entry['AtmosphereType'])

        if 'AtmosphereComposition' in entry:
            for gas in entry['AtmosphereComposition']:
                body_data.add_gas(gas['Name'], gas['Percent'])

        if 'Rings' in entry:
            for ring in entry['Rings']:
                ring_name = ring['Name'][len(entry['BodyName'])+1:]
                body_data.add_ring(ring_name, ring['RingClass'])

    def add_signals(self, entry: Mapping[str, Any]) -> None:
        """
        Add signal data to a planet. This currently only tracks biological signals.

        :param entry: The journal event dict. Must be an event with Signal data.
        """

        body_short_name = self.get_body_name(entry['BodyName'])

        if body_short_name.endswith('Ring') or body_short_name.find('Belt Cluster') != -1:
            return

        body_data = PlanetData.from_journal(self._system, body_short_name, entry['BodyID'], self._session)

        # Add bio signal number just in case
        for signal in entry['Signals']:
            if signal['Type'] == '$SAA_SignalType_Biological;':
                body_data.set_bio_signals(signal['Count'])

        # If signals include genuses, add them to the body data
        if 'Genuses' in entry:
            for genus in entry['Genuses']:
                if body_data.get_flora(genus['Genus']) is None:
                    body_data.add_flora(genus['Genus'])

    def add_scan(self, entry: Mapping[str, Any]) -> None:
        """
        Add scan data to a planet flora. Parse the type and color, if possible.

        :param entry: The journal event dict. Must be a ScanOrganic event.
        """
        planet = self._session.scalar(select(Planet).where(Planet.system_id == self._system.id)
                                      .where(Planet.body_id == entry['Body']))
        if not planet:
            return

        target_body = PlanetData(self._system, planet, self._session)

        scan_level = 0
        match entry['ScanType']:
            case 'Log':
                scan_level = 1
            case 'Sample':
                scan_level = 2
            case 'Analyse':
                scan_level = 3

        if scan_level == 3 and self._cmdr:
            target_body.set_flora_species_scan(
                entry['Genus'], entry['Species'], scan_level, self._cmdr.id
            )

        if 'Variant' in entry:
            _, _, color = parse_variant(entry['Variant'])
            target_body.set_flora_color(entry['Genus'], color)


def get_scan_type(scan: str) -> int:
    match scan:
        case 'AutoScan':
            return 1
        case 'Detailed' | 'NavBeaconDetail':
            return 3
        case 'Basic':
            return 2
        case _:
            return 0


def parse_journal(journal: Path, event: Event) -> int:
    """
    Kickoff function for importing a journal file. Builds a new JournalParse object and begins parsing.

    :param journal: Path object pointing to the journal file
    :param event: Threaded event used to cancel the journal parsing process
    """
    return JournalParse(get_session()).parse_journal(journal, event)


def parse_journals() -> None:
    """
    Journal processing initialization. Creates the process daemon for the main thread and starts the processor.
    """

    if not this.parsing_journals:
        if not this.journal_thread or not this.journal_thread.is_alive():
            this.journal_thread = threading.Thread(target=journal_worker, name='Journal worker')
            this.journal_thread.daemon = True
            this.journal_thread.start()
    else:
        this.journal_stop = True
        if this.journal_event:
            this.journal_event.set()


def journal_sort(journal: Path) -> datetime:
    """
    Sort journals by parsing the name

    :param journal:  Journal Path object
    :return: datetime for the parsed journal date
    """

    match = JOURNAL1_REGEX.search(journal.name)
    if match:
        return datetime(int(f'20{match.group(2)}'), int(match.group(3)), int(match.group(4)), int(match.group(5)),
                        int(match.group(6)), int(match.group(7)), int(match.group(8)))

    match = JOURNAL2_REGEX.search(journal.name)
    if match:
        return datetime(int(match.group(2)), int(match.group(3)), int(match.group(4)), int(match.group(5)),
                        int(match.group(6)), int(match.group(7)), int(match.group(8)))

    return datetime.fromtimestamp(journal.stat().st_ctime)


def journal_worker() -> None:
    """
    Main thread to handle journal importing / processing. Creates up to four additional threads to process each journal
    file and commit to the database. Fires events to update the main TKinter display with the current state.
    """

    journal_dir = config.get_str('journaldir')
    journal_dir = journal_dir if journal_dir else config.default_journal_dir

    journal_dir = expanduser(journal_dir)

    if journal_dir == '':
        return

    this.parsing_journals = True
    this.journal_error = False
    this.journal_progress = (0, 0)
    fire_start_event()

    try:
        journal_files: list[Path] = [Path(journal_dir) / str(x) for x in listdir(journal_dir) if
                                     JOURNAL_REGEX.search(x)]

        if journal_files:
            journal_files = sorted(journal_files, key=journal_sort)
            count = 0
            this.journal_event = threading.Event()
            with concurrent.futures.ThreadPoolExecutor(max_workers=min([cpu_count(), 4])) as executor:
                future_journal: dict[Future, Path] = {executor.submit(parse_journal, journal, this.journal_event):
                                                      journal for journal in journal_files}
                skipped = 0
                for future in concurrent.futures.as_completed(future_journal):
                    count += 1
                    fire_progress_event()
                    if future.result() == 1 or this.journal_stop:
                        if not this.journal_stop:
                            this.journal_error = True
                        this.parsing_journals = False
                        this.journal_event.set()
                        executor.shutdown(wait=True, cancel_futures=True)
                        break
                    elif future.result() == 2:
                        skipped += 1
                    this.journal_progress = (count - skipped, len(journal_files) - skipped)

    except Exception as ex:
        logger.error('Journal parsing failed', exc_info=ex)

    this.parsing_journals = False
    this.journal_stop = False
    this.journal_event = None
    fire_finish_event()


def register_journal_callbacks(frame: tk.Frame, event_name: str, start_func: Optional[Callable],
                               update_func: Optional[Callable], stop_func: Optional[Callable]) -> None:
    """
    Callback registration for journal import events. Can optionally pass a start, update, and stop callback.

    :param frame: The TKinter Frame to attach the event to.
    :param event_name: Unique event identifier for the callback functions
    :param start_func: Optional callback function for start events
    :param update_func: Optional callback function for update events
    :param stop_func: Optional callback function for stop events
    """
    this.journal_processing_callbacks[event_name] = frame
    if start_func:
        frame.bind(f'<<{event_name}_journal_start>>', start_func)
    if update_func:
        frame.bind(f'<<{event_name}_journal_progress>>', update_func)
    if stop_func:
        frame.bind(f'<<{event_name}_journal_finish>>', stop_func)


def fire_start_event() -> None:
    """
    Trigger function for journal import start events. Fires the registered event handlers.
    """
    for name, frame in this.journal_processing_callbacks.items():
        frame.event_generate(f'<<{name}_journal_start>>')


def fire_progress_event() -> None:
    """
    Trigger function for journal import progress events. Fires the registered event handlers.
    """
    for name, frame in this.journal_processing_callbacks.items():
        frame.event_generate(f'<<{name}_journal_progress>>')


def fire_finish_event() -> None:
    """
    Trigger function for journal import stop events. Fires the registered event handlers.
    """
    for name, frame in this.journal_processing_callbacks.items():
        frame.event_generate(f'<<{name}_journal_finish>>')


def register_event_callbacks(events: set[str], func: Callable) -> None:
    """
    Callback registration for main-thread journal processing. Pass a set of events and a function to run when the event
    is found. Passes the journal entry to the event handler callback.

    :param events: A set of event names to be handled by the callback
    :param func: The callback function to attach to the specified events
    """

    for event in events:
        callbacks = this.event_callbacks.get(event, set())
        callbacks.add(func)
        this.event_callbacks[event] = callbacks


def fire_event_callbacks(entry: Mapping[str, Any]):
    """
    Event trigger for registered callbacks. Passes the current journal entry data to any function registered to handle
    that event.

    :param entry: The journal entry data to pass to the callback
    """

    if entry['event'] in this.event_callbacks:
        for func in this.event_callbacks[entry['event']]:
            try:
                func(entry)
            except Exception as ex:
                logger.error('Event callback failed', exc_info=ex)


def shutdown() -> None:
    """
    Journal import shutdown handler. Trigger graceful exit of any active import threads.
    """

    if this.journal_thread and this.journal_thread.is_alive():
        this.journal_stop = True
        if this.journal_event:
            this.journal_event.set()


def has_error() -> bool:
    """
    Helper function to access local data about the journal import error status.
    """

    return this.journal_error


def get_progress() -> tuple[int, int]:
    """
    Helper function to access local data about the journal import progress.
    """

    return this.journal_progress

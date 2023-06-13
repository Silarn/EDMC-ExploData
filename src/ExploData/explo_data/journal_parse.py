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
from .bio_data.codex import parse_variant, set_codex
from .db import System, Commander, Planet, JournalLog, get_session, SystemStatus
from .body_data.struct import PlanetData, StarData

JOURNAL_REGEX = re.compile(r'^Journal(Alpha|Beta)?\.[0-9]{2,4}-?[0-9]{2}-?[0-9]{2}T?[0-9]{2}[0-9]{2}[0-9]{2}'
                           r'\.[0-9]{2}\.log$')
JOURNAL1_REGEX = re.compile(r'^Journal(Alpha|Beta)?\.([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})'
                            r'\.([0-9]){2}\.log$')
JOURNAL2_REGEX = re.compile(r'^Journal(Alpha|Beta)?\.([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})'
                            r'\.([0-9]{2})\.log$')


class This:
    """Holds module globals."""

    def __init__(self):
        self.migration_failed: bool = False
        self.journal_thread: Optional[threading.Thread] = None
        self.parsing_journals: bool = False
        self.journal_stop: bool = False
        self.journal_event: Optional[threading.Event] = None
        self.journal_progress: float = 0.0
        self.journal_error: bool = False

        self.journal_processing_callbacks: dict[str, tk.Frame] = {}
        self.event_callbacks: dict[str, set[Callable]] = {}


this = This()
logger = get_plugin_logger('this.NAME')


class JournalParse:
    def __init__(self, session: Session):
        self._session: Session = session
        self._cmdr: Optional[Commander] = None
        self._system: Optional[System] = None

    def parse_journal(self, journal: Path, event: Event) -> bool:
        if event.is_set():
            return True
        found = self._session.scalar(select(JournalLog).where(JournalLog.journal == journal.name))
        if not found:
            log: BinaryIO = open(journal, 'rb', 0)
            for line in log:
                retry = 2
                while True:
                    result = self.parse_entry(line)
                    if (not result and retry == 0) or event.is_set():
                        return False
                    elif result:
                        break
                    retry -= 1
                    sleep(.1)
        else:
            self._session.expunge(found)
            return True

        journal = JournalLog(journal=journal.name)
        try:
            self._session.add(journal)
            self._session.commit()
        except (IntegrityError, AlcIntegrityError):
            self._session.expunge(journal)
        return True

    def parse_entry(self, line: bytes) -> bool:
        if line is None:
            return False

        try:
            entry: Mapping[str, Any] = json.loads(line)
            self.process_entry(entry)
        except Exception as ex:
            logger.error(f'Invalid journal entry:\n{line!r}\n', exc_info=ex)
            return False
        return True

    def process_entry(self, entry: Mapping[str, Any]):
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
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr)
                if 'StarType' in entry:
                    self.add_star(entry)
                elif 'PlanetClass' in entry:
                    self.add_planet(entry)
            case 'fssdiscoveryscan':
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
                self.add_signals(entry)
            case 'fssallbodiesfound':
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr)
                status = self.get_system_status()
                status.fully_scanned = True
                self._session.commit()
            case 'saascancomplete':
                self._system = self._session.merge(self._system)
                self._cmdr = self._session.merge(self._cmdr)
                body_short_name = self.get_body_name(entry['BodyName'])
                planet: PlanetData = PlanetData.from_journal(self._system, body_short_name,
                                                             entry['BodyID'], self._session)
                target = int(entry['EfficiencyTarget'])
                used = int(entry['ProbesUsed'])
                planet.set_mapped(True, self._cmdr.id).set_efficient(target >= used, self._cmdr.id)
                self._session.commit()
            case 'scanorganic':
                self.add_scan(entry)
            case 'codexentry':
                if entry['Category'] == '$Codex_Category_Biology;' and 'BodyID' in entry:
                    self._system = self._session.merge(self._system)
                    self._cmdr = self._session.merge(self._cmdr)
                    planet: Planet = self._session.scalar(select(Planet).where(Planet.system_id == self._system.id)
                                                          .where(Planet.body_id == entry['BodyID']))
                    if not planet:
                        return

                    target_body = PlanetData(self._system, planet, self._session)

                    genus, species, color = parse_variant(entry['Name'])
                    if genus is not '' and species is not '':
                        target_body.add_flora(genus, species, color)

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
        self._session.commit()
        self._session.close()

        self._cmdr = self._session.scalar(select(Commander).where(Commander.name == name))

        if not self._cmdr:
            self._cmdr = Commander(name=name)
            self._session.add(self._cmdr)
            self._session.commit()

    def set_system(self, name: str, address: list[float]) -> None:
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
        statuses: list[SystemStatus] = self._system.statuses
        statuses = list(filter(lambda item: item.commander_id == self._cmdr.id, statuses))
        if len(statuses):
            status = statuses[0]
        else:
            status = SystemStatus(system_id=self._system.id, commander_id=self._cmdr.id)
            self._session.add(status)
        return status

    def add_star(self, entry: Mapping[str, Any]) -> None:
        """
        Add main star data from journal event

        :param entry: The journal event dict (must be a Scan event with star data)
        """

        body_short_name = self.get_body_name(entry['BodyName'])
        star_data = StarData.from_journal(self._system, body_short_name, entry['BodyID'], self._session)

        star_data.set_distance(entry['DistanceFromArrivalLS']).set_type(entry['StarType']) \
            .set_mass(entry['StellarMass']).set_subclass(entry['Subclass']).set_luminosity(entry['Luminosity']) \
            .set_discovered(True, self._cmdr.id).set_was_discovered(entry['WasDiscovered'], self._cmdr.id)

    def add_planet(self, entry: Mapping[str, Any]) -> None:
        body_short_name = self.get_body_name(entry['BodyName'])
        body_data = PlanetData.from_journal(self._system, body_short_name, entry['BodyID'], self._session)
        body_data.set_distance(float(entry['DistanceFromArrivalLS'])).set_type(entry['PlanetClass']) \
            .set_mass(entry['MassEM']).set_gravity(entry['SurfaceGravity']) \
            .set_temp(entry.get('SurfaceTemperature', None)).set_volcanism(entry.get('Volcanism', None)) \
            .set_terraform_state(entry.get('TerraformState', None)).set_discovered(True, self._cmdr.id) \
            .set_was_discovered(entry['WasDiscovered'], self._cmdr.id).set_was_mapped(entry['WasMapped'], self._cmdr.id)

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

    def add_signals(self, entry: Mapping[str, Any]) -> None:
        body_short_name = self.get_body_name(entry['BodyName'])

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
        self._system = self._session.merge(self._system)
        self._cmdr = self._session.merge(self._cmdr)
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

        if scan_level == 3:
            target_body.set_flora_species_scan(
                entry['Genus'], entry['Species'], scan_level, self._cmdr.id
            )

        if 'Variant' in entry:
            _, _, color = parse_variant(entry['Variant'])
            target_body.set_flora_color(entry['Genus'], color)


def parse_journal(journal: Path, event: Event) -> bool:
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
    this.journal_progress = 0.0
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
                for future in concurrent.futures.as_completed(future_journal):
                    count += 1
                    this.journal_progress = count / len(journal_files)
                    fire_progress_event()
                    if not future.result() or this.journal_stop:
                        if not this.journal_stop:
                            this.journal_error = True
                        this.parsing_journals = False
                        this.journal_event.set()
                        executor.shutdown(wait=True, cancel_futures=True)
                        break

    except Exception as ex:
        logger.error('Journal parsing failed', exc_info=ex)

    this.parsing_journals = False
    this.journal_stop = False
    this.journal_event = None
    fire_finish_event()


def register_journal_callbacks(frame: tk.Frame, event_name: str, start_func: Optional[Callable],
                               update_func: Optional[Callable], stop_func: Optional[Callable]) -> None:
    this.journal_processing_callbacks[event_name] = frame
    if start_func:
        frame.bind(f'<<{event_name}_journal_start>>', start_func)
    if update_func:
        frame.bind(f'<<{event_name}_journal_progress>>', update_func)
    if stop_func:
        frame.bind(f'<<{event_name}_journal_finish>>', stop_func)


def fire_start_event() -> None:
    for name, frame in this.journal_processing_callbacks.items():
        frame.event_generate(f'<<{name}_journal_start>>')


def fire_progress_event() -> None:
    for name, frame in this.journal_processing_callbacks.items():
        frame.event_generate(f'<<{name}_journal_progress>>')


def fire_finish_event() -> None:
    for name, frame in this.journal_processing_callbacks.items():
        frame.event_generate(f'<<{name}_journal_finish>>')


def register_event_callbacks(events: set[str], func: Callable) -> None:
    for event in events:
        callbacks = this.event_callbacks.get('event', set())
        callbacks.add(func)
        this.event_callbacks[event] = callbacks


def fire_event_callbacks(entry: Mapping[str, Any]):
    if entry['event'] in this.event_callbacks:
        for func in this.event_callbacks[entry['event']]:
            try:
                func(entry)
            except Exception as ex:
                logger.error('Event callback failed', exc_info=ex)


def shutdown() -> None:
    if this.journal_thread and this.journal_thread.is_alive():
        this.journal_stop = True
        if this.journal_event:
            this.journal_event.set()


def has_error() -> bool:
    return this.journal_error


def get_progress() -> float:
    return this.journal_progress

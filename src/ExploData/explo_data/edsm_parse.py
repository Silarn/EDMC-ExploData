# -*- coding: utf-8 -*-
# ExploData module plugin for EDMC
# Source: https://github.com/Silarn/EDMC-ExploData
# Licensed under the [GNU Public License (GPL)](http://www.gnu.org/licenses/gpl-2.0.html) version 2 or later.

import re
import requests
import threading
import tkinter as tk
from typing import Callable, Mapping, Optional
from urllib.parse import quote

from sqlalchemy import select
from sqlalchemy.orm import Session

from EDMCLogging import get_plugin_logger

from ExploData.explo_data import const
from .db import System, get_session, Star
from .body_data.struct import PlanetData, StarData
from .body_data.edsm import parse_edsm_star_class, parse_edsm_ring_class, map_edsm_type, map_edsm_atmosphere


class This:
    """Holds globals."""

    def __init__(self):
        self.edsm_fetch_callbacks: dict[str, tk.Frame] = {}
        self.event_callbacks: dict[str, set[Callable]] = {}
        self.edsm_thread: threading.Thread | None = None


this = This()
logger = get_plugin_logger(const.plugin_name)


def get_body_name(system_name: str, fullname: str) -> str:
    """
    Remove the base system name from the body name if the body has a unique identifier.
    Usually only the main star has the same name as the system in one-star systems.

    :param system_name: The name of the system the body resides in
    :param fullname: The full name of the body including the system name
    :return: The short name of the body unless it matches the system name
    """

    if fullname.startswith(system_name + ' '):
        body_name = fullname[len(system_name + ' '):]
    else:
        body_name = fullname
    return body_name


class EDSMFetch:
    """
    This class is a general purpose container to process EDSM requests.
    """
    def __init__(self, session: Session):
        self._session: Session = session
        self._system: Optional[System] = None
        self._edsm_session: str | None = None
        self._edsm_bodies: Mapping | None = None

    def edsm_fetch(self, system_name: str) -> None:
        """ EDSM system data fetch thread initialization """
    
        if not this.edsm_thread or not this.edsm_thread.is_alive():
            fire_start_event()
            this.edsm_thread = threading.Thread(target=self.edsm_worker, name='EDSM worker', args=(system_name,))
            this.edsm_thread.daemon = True
            this.edsm_thread.start()

    def edsm_worker(self, system_name: str) -> None:
        """ Fetch system data from EDSM on a threaded function """
    
        if not self._edsm_session:
            self._edsm_session = requests.Session()
    
        try:
            r = self._edsm_session.get('https://www.edsm.net/api-system-v1/bodies?systemName=%s' % quote(system_name),
                                      timeout=10)
            r.raise_for_status()
            self._edsm_bodies = r.json() or {}
        except requests.exceptions.RequestException:
            self._edsm_bodies = None
    
        self.process_edsm_data()

    def process_edsm_data(self) -> None:
        """ Handle data retrieved from EDSM """
    
        if self._edsm_bodies is None:
            return
        
        system_name = self._edsm_bodies.get('name', '')
        self._system = self._session.scalar(select(System).where(System.name == system_name))
        if not self._system:
            self._system = System(name=system_name)
            self._session.add(self._system)
        self._session.commit()
    
        for body in self._edsm_bodies.get('bodies', []):
            body_short_name = get_body_name(system_name, body['name'])
            if body['type'] == 'Star':
                self.add_edsm_star(body)
            elif body['type'] == 'Planet':
                self._system = self._session.merge(self._system)
                try:
                    planet_data = PlanetData.from_journal(self._system, body_short_name, body['bodyId'], self._session)
                    planet_type = map_edsm_type(body['subType'])
                    terraformable = 'Terraformable' if body['terraformingState'] == 'Candidate for terraforming' \
                        else ''
                    planet_data.set_type(planet_type) \
                        .set_distance(body['distanceToArrival']) \
                        .set_atmosphere(map_edsm_atmosphere(body['atmosphereType'])) \
                        .set_gravity(body['gravity'] * 9.797759) \
                        .set_temp(body['surfaceTemperature']) \
                        .set_mass(body['earthMasses']) \
                        .set_terraform_state(terraformable) \
                        .set_landable(body['isLandable']) \
                        .set_orbital_period(body['orbitalPeriod'] * 86400 if body['orbitalPeriod'] else 0) \
                        .set_rotation(body['rotationalPeriod'] * 86400)
                    if body['volcanismType'] == 'No volcanism':
                        volcanism = ''
                    else:
                        volcanism = body['volcanismType'].lower().capitalize() + ' volcanism'
                    planet_data.set_volcanism(volcanism)
    
                    star_search = re.search('^([A-Z]+) .+$', body_short_name)
                    if star_search:
                        for star in star_search.group(1):
                            planet_data.add_parent_star(star)
                    else:
                        planet_data.add_parent_star(self._system.name)
    
                    if 'materials' in body:
                        for material in body['materials']:  # type: str
                            planet_data.add_material(material.lower())
    
                    atmosphere_composition: dict[str, float] = body.get('atmosphereComposition', {})
                    if atmosphere_composition:
                        for gas, percent in atmosphere_composition.items():
                            planet_data.add_gas(map_edsm_atmosphere(gas), percent)

                    if 'rings' in body:
                        for ring in body['rings']:
                            ring_name = ring['name'][len(body['name'])+1:]
                            planet_data.add_ring(ring_name, parse_edsm_ring_class(ring['type']))
    
                except Exception as e:
                    logger.error('Error while parsing EDSM', exc_info=e)

        self._session.commit()
        fire_finish_event()

    def add_edsm_star(self, body: dict) -> None:
        """
        Add a parent star from EDSM API data
    
        :param body: The EDSM body data (JSON)
        """

        self._system = self._session.merge(self._system)
        try:
            body_short_name = get_body_name(self._system.name, body['name'])
            star_data = StarData.from_journal(self._system, body_short_name, body['bodyId'], self._session)
            if body['spectralClass']:
                star_data.set_type(body['spectralClass'][:-1])
                star_data.set_subclass(body['spectralClass'][-1])
            else:
                star_data.set_type(parse_edsm_star_class(body['subType']))
            star_data.set_luminosity(body['luminosity'])
            star_data.set_distance(body['distanceToArrival'])
            star_data.set_mass(body['solarMasses'])
            star_data.set_orbital_period(body['orbitalPeriod'] * 86400 if body['orbitalPeriod'] else 0)
            star_data.set_rotation(body['rotationalPeriod'] * 86400)
            for ring_type in ['belts', 'rings']:
                if ring_type in body:
                    for belt in body[ring_type]:
                        ring_name = belt['name'][len(body['name'])+1:]
                        star_data.add_ring(ring_name, parse_edsm_ring_class(belt['type']))
        except Exception as e:
            logger.error('Error while parsing EDSM', exc_info=e)

    def get_main_star(self) -> Optional[Star]:
        if self._system and self._system.id:
            return self._session.scalar(select(Star).where(Star.system_id == self._system.id).where(Star.distance == 0.0))
        return None
    
    
def edsm_fetch(system_name: str) -> None:
    EDSMFetch(get_session()).edsm_fetch(system_name)


def register_edsm_callbacks(frame: tk.Frame, event_name: str, start_func: Optional[Callable],
                               stop_func: Optional[Callable]) -> None:
    """
    Callback registration for EDSM fetch events. Can optionally pass a start, update, and stop callback.

    :param frame: The TKinter Frame to attach the event to.
    :param event_name: Unique event identifier for the callback functions
    :param start_func: Optional callback function for start events
    :param stop_func: Optional callback function for stop events
    """
    this.edsm_fetch_callbacks[event_name] = frame
    if start_func:
        frame.bind(f'<<{event_name}_edsm_start>>', start_func)
    if stop_func:
        frame.bind(f'<<{event_name}_edsm_finish>>', stop_func)


def fire_start_event() -> None:
    """
    Trigger function for EDSM fetch start events. Fires the registered event handlers.
    """
    for name, frame in this.edsm_fetch_callbacks.items():
        frame.event_generate(f'<<{name}_edsm_start>>')


def fire_finish_event() -> None:
    """
    Trigger function for EDSM fetch stop events. Fires the registered event handlers.
    """
    for name, frame in this.edsm_fetch_callbacks.items():
        frame.event_generate(f'<<{name}_edsm_finish>>')


def shutdown() -> None:
    """
    Journal import shutdown handler. Trigger graceful exit of any active import threads.
    """

    if this.edsm_thread and this.edsm_thread.is_alive():
        this.edsm_thread.join()

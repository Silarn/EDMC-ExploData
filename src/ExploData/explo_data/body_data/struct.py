# -*- coding: utf-8 -*-
# ExploData module plugin for EDMC
# Source: https://github.com/Silarn/EDMC-ExploData
# Licensed under the [GNU Public License (GPL)](http://www.gnu.org/licenses/gpl-2.0.html) version 2 or later.

from typing import Self, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from ..db import Planet, System, PlanetFlora, PlanetGas, PlanetRing, PlanetStatus, Waypoint, FloraScans, Star, \
    StarRing, StarStatus, NonBody, NonBodyStatus


class PlanetData:
    """ Holds all attributes, getters, and setters for planet data. """

    def __init__(self, system: System, data: Planet, session: Session):
        self._session: Session = session
        self._system: System = system
        self._data: Planet = data

    @classmethod
    def from_journal(cls, system: System, name: str, body_id: int, session: Session):
        data: Planet = session.scalar(select(Planet).where(Planet.name == name).where(Planet.system_id == system.id))
        if not data:
            data = Planet(name=name, body_id=body_id, system_id=system.id)
            session.add(data)
        session.commit()

        return cls(system, data, session)

    def get_name(self) -> str:
        return self._data.name

    def get_type(self) -> str:
        return self._data.type

    def set_type(self, value: str) -> Self:
        self._data.type = value
        self.commit()
        return self

    def get_id(self) -> int:
        return self._data.body_id

    def set_id(self, value: int) -> Self:
        self._data.body_id = value
        self.commit()
        return self

    def get_status(self, commander_id: int) -> PlanetStatus:
        statuses: list[PlanetStatus] = self._data.statuses
        statuses = list(filter(lambda item: item.commander_id == commander_id, statuses))
        if len(statuses):
            status = statuses[0]
        else:
            status = PlanetStatus(planet_id=self._data.id, commander_id=commander_id)
            self._data.statuses.append(status)
            self._session.commit()
        return status

    def get_atmosphere(self) -> str:
        return self._data.atmosphere

    def set_atmosphere(self, value: str) -> Self:
        self._data.atmosphere = value
        self.commit()
        return self

    def add_gas(self, gas: str, percent: float) -> Self:
        for gas_data in self._data.gasses:  # type: PlanetGas
            if gas_data.gas_name == gas:
                gas_data.percent = percent
                self.commit()
                return self
        self._data.gasses.append(PlanetGas(gas_name=gas, percent=percent))
        self.commit()
        return self

    def get_gas(self, gas: str) -> float:
        for gas_data in self._data.gasses:  # type: PlanetGas
            if gas_data.gas_name == gas:
                return gas_data.percent
        return 0.0

    def get_volcanism(self) -> str:
        return self._data.volcanism

    def set_volcanism(self, value: Optional[int]) -> Self:
        self._data.volcanism = value
        self.commit()
        return self

    def get_distance(self) -> Optional[float]:
        return self._data.distance

    def set_distance(self, value: float) -> Self:
        self._data.distance = value
        self.commit()
        return self

    def get_gravity(self) -> float:
        return self._data.gravity

    def set_gravity(self, value: float) -> Self:
        self._data.gravity = value
        self.commit()
        return self

    def get_mass(self) -> Optional[float]:
        return self._data.mass

    def set_mass(self, value: float) -> Self:
        self._data.mass = value
        self.commit()
        return self

    def get_rotation(self) -> Optional[float]:
        return self._data.rotation

    def set_rotation(self, value: float) -> Self:
        self._data.rotation = value
        self.commit()
        return self

    def get_orbital_period(self) -> Optional[float]:
        return self._data.orbital_period

    def set_orbital_period(self, value: float) -> Self:
        self._data.orbital_period = value
        self.commit()
        return self

    def get_temp(self) -> Optional[float]:
        return self._data.temp

    def set_temp(self, value: Optional[float]) -> Self:
        self._data.temp = value
        self.commit()
        return self

    def get_pressure(self) -> Optional[float]:
        return self._data.pressure

    def set_pressure(self, value: Optional[float]) -> Self:
        self._data.pressure = value
        self.commit()
        return self

    def get_radius(self) -> float:
        return self._data.radius

    def set_radius(self, value: float) -> Self:
        self._data.radius = value
        self.commit()
        return self

    def get_bio_signals(self) -> int:
        return self._data.bio_signals

    def set_bio_signals(self, value: int) -> Self:
        self._data.bio_signals = value
        self.commit()
        return self

    def get_parent_stars(self) -> list[str]:
        if self._data.parent_stars:
            return self._data.parent_stars.split(',')
        return []

    def add_parent_star(self, value: str) -> Self:
        if self._data.parent_stars:
            stars: list[str] = []
            stars += self._data.parent_stars.split(',')
            stars.append(value)
            sorted_stars = sorted(set(stars))
            self._data.parent_stars = ','.join(sorted_stars)
        else:
            self._data.parent_stars = value
        self.commit()
        return self

    def get_flora(self, genus: str = None, species: str = None, create: bool = False) -> list[PlanetFlora] | None:
        if genus:
            flora_list: list[PlanetFlora] = []
            for flora in self._data.floras:  # type: PlanetFlora
                if flora.genus == genus:
                    if flora.species == '':
                        return [flora]
                    if species and species != '':
                        if flora.species == species:
                            return [flora]
                    else:
                        flora_list.append(flora)
            if not len(flora_list):
                if create:
                    new_flora = PlanetFlora(genus=genus)
                    if species:
                        new_flora.species = species
                    self._data.floras.append(new_flora)
                    self.commit()
                    return [new_flora]
                return None
            else:
                return flora_list
        return self._data.floras

    def add_flora(self, genus: str, species: str = '', color: str = '') -> Self:
        flora = self.get_flora(genus, species, create=True)[0]
        flora.species = species
        flora.color = color
        self.commit()
        return self

    def set_flora_species_scan(self, genus: str, species: str, scan: int, commander: int) -> Self:
        flora = self.get_flora(genus, species, create=True)[0]
        flora.species = species
        stmt = select(FloraScans).where(FloraScans.flora_id == flora.id).where(FloraScans.commander_id == commander)
        scan_data: Optional[FloraScans] = self._session.scalar(stmt)
        if not scan_data:
            scan_data = FloraScans(flora_id=flora.id, commander_id=commander)
            self._session.add(scan_data)
            self.commit()
        scan_data.count = scan
        if scan == 3:
            stmt = delete(Waypoint).where(Waypoint.commander_id == commander).where(Waypoint.flora_id == flora.id)
            self._session.execute(stmt)
        self.commit()
        return self

    def set_flora_color(self, genus: str, color: str) -> Self:
        flora = self.get_flora(genus, create=True)[0]
        flora.color = color
        self.commit()
        return self

    def add_flora_waypoint(self, genus: str, species: str, lat_long: tuple[float, float], commander: int, scan: bool = False) -> Self:
        flora = self.get_flora(genus, species)[0]
        if flora:
            scans: FloraScans = self._session.scalar(
                select(FloraScans).where(FloraScans.flora_id == flora.id).where(FloraScans.commander_id == commander)
            )
            if not scans or scans.count != 3:
                waypoint = Waypoint()
                waypoint.flora_id = flora.id
                waypoint.commander_id = commander
                waypoint.latitude = lat_long[0]
                waypoint.longitude = lat_long[1]
                if scan:
                    waypoint.type = 'scan'
                self._session.add(waypoint)
                self.commit()
        return self

    def has_waypoint(self, commander: id) -> bool:
        for flora in self._data.floras:  # type: PlanetFlora
            stmt = select(Waypoint) \
                .where(Waypoint.flora_id == flora.id) \
                .where(Waypoint.commander_id == commander) \
                .where(Waypoint.type == 'tag')
            if self._session.scalars(stmt):
                return True
        return False

    def get_materials(self) -> set[str]:
        if self._data.materials:
            return set(self._data.materials.split(','))
        return set()

    def add_material(self, material: str) -> Self:
        materials = self.get_materials()
        materials.add(material)
        self._data.materials = ','.join(materials)
        self.commit()
        return self

    def get_rings(self) -> list[PlanetRing] | None:
        return self._data.rings

    def add_ring(self, name: str, ring_type: str = '') -> Self:
        for ring in self._data.rings:  # type: PlanetRing
            if ring.name == name:
                ring.type = ring_type
                self.commit()
                return self
        new_ring = PlanetRing(name=name, type=ring_type)
        self._data.rings.append(new_ring)
        self.commit()
        return self

    def clear_flora(self) -> Self:
        self._session.execute(delete(PlanetFlora).where(PlanetFlora.planet_id == self._data.id))
        self.commit()
        return self

    def is_landable(self) -> bool:
        return self._data.landable

    def set_landable(self, value: bool) -> Self:
        self._data.landable = value
        self.commit()
        return self

    def get_terraform_state(self) -> str:
        return self._data.terraform_state

    def set_terraform_state(self, value: str) -> Self:
        self._data.terraform_state = value
        self.commit()
        return self

    def is_terraformable(self) -> bool:
        if self._data.terraform_state and self._data.terraform_state in ['Terraformable', 'Terraforming', 'Terraformed']:
            return True
        return False

    def get_scan_state(self, commander_id: int) -> int:
        status = self.get_status(commander_id)
        return status.scan_state

    def set_scan_state(self, value: int, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        if value > status.scan_state:
            status.scan_state = value
        self.commit()
        return self

    def is_discovered(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.discovered

    def set_discovered(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.discovered = value
        self.commit()
        return self

    def was_discovered(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.was_discovered

    def set_was_discovered(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.was_discovered = value
        self.commit()
        return self

    def is_mapped(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.mapped

    def set_mapped(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.mapped = value
        self.commit()
        return self

    def was_mapped(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.was_mapped

    def set_was_mapped(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.was_mapped = value
        self.commit()
        return self

    def was_efficient(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.efficient

    def set_efficient(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.efficient = value
        self.commit()
        return self

    def commit(self) -> None:
        self._session.commit()

    def refresh(self) -> None:
        self._session.refresh(self._system)
        self._session.refresh(self._data)

    def __del__(self) -> None:
        self.commit()


class NonBodyData:
    """ Holds all attributes, getters, and setters for star data. """

    def __init__(self, system: System, data: NonBody, session: Session):
        self._session: Session = session
        self._system: System = system
        self._data: NonBody = data

    @classmethod
    def from_journal(cls, system: System, name: str, body_id: int, session: Session):
        data: NonBody = session.scalar(
            select(NonBody).where(NonBody.name == name).where(NonBody.system_id == system.id)
        )
        if not data:
            data = NonBody()
            data.name = name
            data.system_id = system.id
            data.body_id = body_id
            session.add(data)
        session.commit()

        return cls(system, data, session)

    def get_name(self) -> str:
        return self._data.name

    def get_id(self) -> int:
        return self._data.body_id

    def get_status(self, commander_id: int) -> NonBodyStatus:
        statuses: list[NonBodyStatus] = self._data.statuses
        statuses = list(filter(lambda item: item.commander_id == commander_id, statuses))
        if len(statuses):
            status = statuses[0]
        else:
            status = NonBodyStatus(non_body_id=self._data.id, commander_id=commander_id)
            self._data.statuses.append(status)
            self._session.commit()
        return status

    def is_discovered(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.discovered

    def set_discovered(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.discovered = value
        self.commit()
        return self

    def was_discovered(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.was_discovered

    def set_was_discovered(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.was_discovered = value
        self.commit()
        return self

    def is_mapped(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.mapped

    def set_mapped(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.mapped = value
        self.commit()
        return self

    def was_mapped(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.was_mapped

    def set_was_mapped(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.was_mapped = value
        self.commit()
        return self

    def was_efficient(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.efficient

    def set_efficient(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.efficient = value
        self.commit()
        return self

    def refresh(self) -> None:
        self._session.refresh(self._system)
        self._session.refresh(self._data)

    def commit(self) -> None:
        self._session.commit()

    def __del__(self) -> None:
        self.commit()


class StarData:
    """ Holds all attributes, getters, and setters for star data. """

    def __init__(self, system: System, data: Star, session: Session):
        self._session = session
        self._system = system
        self._data = data

    @classmethod
    def from_journal(cls, system: System, name: str, body_id: int, session: Session):
        data: Star = session.scalar(
            select(Star).where(Star.name == name).where(Star.system_id == system.id)
        )
        if not data:
            data = Star()
            data.name = name
            data.system_id = system.id
            data.body_id = body_id
            session.add(data)
        session.commit()

        return cls(system, data, session)

    def get_name(self) -> str:
        return self._data.name

    def get_id(self) -> int:
        return self._data.body_id

    def get_status(self, commander_id: int) -> StarStatus:
        statuses: list[StarStatus] = self._data.statuses
        statuses = list(filter(lambda item: item.commander_id == commander_id, statuses))
        if len(statuses):
            status = statuses[0]
        else:
            status = StarStatus(star_id=self._data.id, commander_id=commander_id)
            self._session.add(status)
            self._session.commit()
        return status

    def get_distance(self) -> Optional[float]:
        return self._data.distance

    def set_distance(self, value: float) -> Self:
        self._data.distance = value
        self.commit()
        return self

    def get_mass(self) -> Optional[float]:
        return self._data.mass

    def set_mass(self, value: float) -> Self:
        self._data.mass = value
        self.commit()
        return self

    def get_rotation(self) -> Optional[float]:
        return self._data.rotation

    def set_rotation(self, value: float) -> Self:
        self._data.rotation = value
        self.commit()
        return self

    def get_orbital_period(self) -> Optional[float]:
        return self._data.orbital_period

    def set_orbital_period(self, value: float) -> Self:
        self._data.orbital_period = value
        self.commit()
        return self

    def get_type(self) -> str:
        return self._data.type

    def set_type(self, value: str) -> Self:
        self._data.type = value
        self.commit()
        return self

    def get_subclass(self) -> int:
        return self._data.subclass

    def set_subclass(self, value: int) -> Self:
        self._data.subclass = value
        self.commit()
        return self

    def get_luminosity(self) -> str:
        return self._data.luminosity

    def set_luminosity(self, value: str) -> Self:
        self._data.luminosity = value
        self.commit()
        return self

    def get_rings(self) -> list[StarRing] | None:
        return self._data.rings

    def add_ring(self, name: str, ring_type: str = '') -> Self:
        for ring in self._data.rings:  # type: StarRing
            if ring.name == name:
                ring.type = ring_type
                self.commit()
                return self
        new_ring = StarRing(name=name, type=ring_type)
        self._data.rings.append(new_ring)
        self.commit()
        return self

    def is_discovered(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.discovered

    def set_discovered(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.discovered = value
        self.commit()
        return self

    def was_discovered(self, commander_id: int) -> bool:
        status = self.get_status(commander_id)
        return status.was_discovered

    def set_was_discovered(self, value: bool, commander_id: int) -> Self:
        status = self.get_status(commander_id)
        status.was_discovered = value
        self.commit()
        return self

    def refresh(self) -> None:
        self._session.refresh(self._system)
        self._session.refresh(self._data)

    def commit(self) -> None:
        self._session.commit()

    def __del__(self) -> None:
        self.commit()


def load_planets(system: System, session: Session) -> dict[str, PlanetData]:
    planet_data: dict[str, PlanetData] = {}
    if system and system.id:
        for planet in system.planets:  # type: Planet
            planet_data[planet.name] = PlanetData(system, planet, session)
    session.commit()
    return planet_data


def load_non_bodies(system: System, session: Session) -> dict[str, NonBodyData]:
    non_body_data: dict[str, NonBodyData] = {}
    if system and system.id:
        for non_body in system.non_bodies:  # type: NonBody
            non_body_data[non_body.name] = NonBodyData(system, non_body, session)
    return non_body_data


def load_stars(system: System, session: Session) -> dict[str, StarData]:
    star_data: dict[str, StarData] = {}
    if system and system.id:
        for star in system.stars:  # type: Star
            star_data[star.name] = StarData(system, star, session)
    session.commit()
    return star_data


def get_main_star(system: System, session: Session) -> Optional[Star]:
    if system and system.id:
        return session.scalar(select(Star).where(Star.system_id == system.id).where(Star.distance == 0.0))
    return None

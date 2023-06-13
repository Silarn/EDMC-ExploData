"""
The database structure models and helper functions for ExploData data
"""

import threading
from sqlite3 import OperationalError
from typing import Optional

import sqlalchemy.exc
from sqlalchemy import ForeignKey, String, UniqueConstraint, select, Column, Float, Engine, text, Integer, Table, \
    MetaData, Executable, Result, create_engine, Boolean
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, scoped_session, sessionmaker, Session
from sqlalchemy.sql.ddl import CreateTable

from .const import database_version, plugin_name

from EDMCLogging import get_plugin_logger
from config import config

logger = get_plugin_logger(plugin_name)


class This:
    """Holds module globals."""

    def __init__(self):
        self.sql_engine: Optional[Engine] = None
        self.sql_session_factory: Optional[scoped_session] = None
        self.migration_failed: bool = False
        self.journal_thread: Optional[threading.Thread] = None
        self.parsing_journals: bool = False
        self.journal_stop: bool = False
        self.journal_event: Optional[threading.Event] = None
        self.journal_progress: float = 0.0
        self.journal_error: bool = False


this = This()


class Base(DeclarativeBase):
    pass


class Commander(Base):
    __tablename__ = 'commanders'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(22), unique=True)


class Metadata(Base):
    __tablename__ = 'metadata'

    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(default='')


class System(Base):
    """ DB model for system data """
    __tablename__ = 'systems'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    statuses: Mapped[list['SystemStatus']] = relationship(
        back_populates='status', cascade='all, delete-orphan'
    )
    x: Mapped[float] = mapped_column(default=0.0)
    y: Mapped[float] = mapped_column(default=0.0)
    z: Mapped[float] = mapped_column(default=0.0)
    region: Mapped[Optional[int]]
    body_count: Mapped[int] = mapped_column(default=1)
    non_body_count: Mapped[int] = mapped_column(default=0)

    planets: Mapped[list['Planet']] = relationship(
        back_populates='planet', cascade='all, delete-orphan'
    )

    stars: Mapped[list['Star']] = relationship(
        back_populates='star', cascade='all, delete-orphan'
    )


class SystemStatus(Base):
    __tablename__ = 'system_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id'))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id'))
    status: Mapped['System'] = relationship(back_populates='statuses')
    honked: Mapped[bool] = mapped_column(default=False)
    fully_scanned: Mapped[bool] = mapped_column(default=False)
    fully_mapped: Mapped[bool] = mapped_column(default=False)
    __table_args__ = (UniqueConstraint('system_id', 'commander_id', name='_system_commander_constraint'),
                      )


class Planet(Base):
    """ DB model for planet data """
    __tablename__ = 'planets'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id'))
    planet: Mapped[list['System']] = relationship(back_populates='planets')
    name: Mapped[str] = mapped_column(String(32))
    type: Mapped[str] = mapped_column(String(32), default='')
    body_id: Mapped[int]
    statuses: Mapped[list['PlanetStatus']] = relationship(
        back_populates='status', cascade='all, delete-orphan'
    )
    atmosphere: Mapped[str] = mapped_column(String(32), default='')
    gasses: Mapped[list['PlanetGas']] = relationship(
        back_populates='gas', cascade='all, delete-orphan'
    )
    volcanism: Mapped[Optional[str]] = mapped_column(String(32))
    distance: Mapped[float] = mapped_column(default=0.0)
    mass: Mapped[float] = mapped_column(default=0.0)
    gravity: Mapped[float] = mapped_column(default=0.0)
    temp: Mapped[Optional[float]]
    parent_stars: Mapped[str] = mapped_column(default='')
    bio_signals: Mapped[int] = mapped_column(default=0)
    floras: Mapped[list['PlanetFlora']] = relationship(
        back_populates='flora', cascade='all, delete-orphan'
    )
    materials: Mapped[str] = mapped_column(default='')
    terraform_state: Mapped[str] = mapped_column(default='')
    __table_args__ = (UniqueConstraint('system_id', 'name', 'body_id', name='_system_name_id_constraint'),
                      )


class PlanetStatus(Base):
    __tablename__ = 'planet_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    planet_id: Mapped[int] = mapped_column(ForeignKey('planets.id'))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id'))
    status: Mapped['Planet'] = relationship(back_populates='statuses')
    discovered: Mapped[bool] = mapped_column(default=True)
    was_discovered: Mapped[bool] = mapped_column(default=False)
    mapped: Mapped[bool] = mapped_column(default=False)
    was_mapped: Mapped[bool] = mapped_column(default=False)
    efficient: Mapped[bool] = mapped_column(default=False)
    __table_args__ = (UniqueConstraint('planet_id', 'commander_id', name='_planet_commander_constraint'),
                      )


class PlanetGas(Base):
    __tablename__ = 'planet_gasses'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    planet_id: Mapped[int] = mapped_column(ForeignKey('planets.id'))
    gas: Mapped['Planet'] = relationship(back_populates='gasses')
    gas_name: Mapped[str]
    percent: Mapped[float]
    __table_args__ = (UniqueConstraint('planet_id', 'gas_name', name='_planet_gas_constraint'), )

    def __repr__(self) -> str:
        return f'PlanetGas(gas_name={self.gas_name!r}, percent={self.percent!r})'


class PlanetFlora(Base):
    __tablename__ = 'planet_flora'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    planet_id: Mapped[int] = mapped_column(ForeignKey('planets.id'))
    flora: Mapped['Planet'] = relationship(back_populates='floras')
    scans: Mapped[list['FloraScans']] = relationship(back_populates='scan', cascade='all, delete-orphan')
    waypoints: Mapped[list['Waypoint']] = relationship(back_populates='waypoint', cascade='all, delete-orphan')
    genus: Mapped[str]
    species: Mapped[str] = mapped_column(default='')
    color: Mapped[str] = mapped_column(default='')
    __table_args__ = (UniqueConstraint('planet_id', 'genus', name='_planet_genus_constraint'),
                      )


class FloraScans(Base):
    __tablename__ = 'flora_scans'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id'))
    flora_id: Mapped[int] = mapped_column(ForeignKey('planet_flora.id'))
    scan: Mapped['PlanetFlora'] = relationship(back_populates='scans')
    count: Mapped[int] = mapped_column(default=0)
    __table_args__ = (UniqueConstraint('commander_id', 'flora_id', name='_cmdr_flora_constraint'),
                      )


class Waypoint(Base):
    __tablename__ = 'flora_waypoints'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id'))
    flora_id: Mapped[int] = mapped_column(ForeignKey('planet_flora.id'))
    waypoint: Mapped['PlanetFlora'] = relationship(back_populates='waypoints')
    type: Mapped[str] = mapped_column(default='tag')
    latitude: Mapped[float]
    longitude: Mapped[float]


class Star(Base):
    """ DB model for star data """
    __tablename__ = 'stars'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id'))
    star: Mapped[list['System']] = relationship(back_populates='stars')
    name: Mapped[str]
    body_id: Mapped[int]
    statuses: Mapped[list['StarStatus']] = relationship(
        back_populates='status', cascade='all, delete-orphan'
    )
    distance: Mapped[Optional[float]]
    mass: Mapped[float] = mapped_column(default=0.0)
    type: Mapped[str] = mapped_column(default='')
    subclass: Mapped[int] = mapped_column(default=0)
    luminosity: Mapped[str] = mapped_column(default='')
    __table_args__ = (UniqueConstraint('system_id', 'name', 'body_id', name='_system_name_id_constraint'),
                      )


class StarStatus(Base):
    __tablename__ = 'star_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    star_id: Mapped[int] = mapped_column(ForeignKey('stars.id'))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id'))
    status: Mapped['Star'] = relationship(back_populates='statuses')
    discovered: Mapped[bool] = mapped_column(default=True)
    was_discovered: Mapped[bool] = mapped_column(default=False)
    __table_args__ = (UniqueConstraint('star_id', 'commander_id', name='_star_commander_constraint'),
                      )


class CodexScans(Base):
    __tablename__ = 'codex_scans'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id'))
    region: Mapped[int]
    biological: Mapped[str] = mapped_column(default='')
    __table_args__ = (UniqueConstraint('commander_id', 'region', 'biological', name='_cmdr_bio_region_constraint'),)


class JournalLog(Base):
    __tablename__ = 'journal_log'

    journal: Mapped[str] = mapped_column(String(32), primary_key=True)


def modify_table(engine: Engine, table: type[Base]):
    new_table_name = f'{table.__tablename__}_new'
    statement = text(f'DROP TABLE IF EXISTS {new_table_name}')  # drop table left over from failed migration
    run_statement(engine, statement)
    run_query(engine, 'PRAGMA foreign_keys=off')
    metadata = MetaData()
    columns: list[Column] = [column.copy() for column in table.__table__.columns.values()]
    column_names: list[str] = table.__table__.columns.keys()
    args = []
    if hasattr(table, '__table_args__'):
        for arg in table.__table_args__:
            if type(arg) == UniqueConstraint:
                args.append(arg.copy())
            else:
                args.append(arg)
    new_table = Table(new_table_name, metadata, *columns, *args)
    statement = text(str(CreateTable(new_table).compile(engine)))
    run_statement(engine, statement)
    statement = text(f'INSERT INTO `{new_table_name}` ({", ".join(column_names)}) SELECT {", ".join(column_names)} FROM `{table.__tablename__}`')
    run_statement(engine, statement)
    statement = text(f'DROP TABLE `{table.__tablename__}`')
    run_statement(engine, statement)
    statement = text(f'ALTER TABLE `{new_table_name}` RENAME TO `{table.__tablename__}`')
    run_statement(engine, statement)
    run_query(engine, 'PRAGMA foreign_keys=on')


def add_column(engine: Engine, table_name: str, column: Column):
    column_name = column.compile(dialect=engine.dialect)
    column_type = column.type.compile(engine.dialect)
    default: Optional[ColumnDefault] = column.default
    default_arg: any = default.arg if default.has_arg else None
    if type(default_arg) is str:
        default_arg = f"'{default_arg}'"
    null_text = ' NOT NULL' if not column.nullable else ''
    default_text = f' DEFAULT {default_arg}' if default_arg is not None else ''

    try:
        statement = text(f'ALTER TABLE {table_name} DROP COLUMN {column_name}')
        run_statement(engine, statement)
    except (OperationalError, sqlalchemy.exc.OperationalError):
        pass
    statement = text(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}{null_text}{default_text}')
    run_statement(engine, statement)


def run_query(engine: Engine, query: str) -> Result:
    return run_statement(engine, text(query))


def run_statement(engine: Engine, statement: Executable) -> Result:
    connection = engine.connect()
    result = connection.execute(statement)
    connection.commit()
    connection.close()
    return result


def migrate(engine: Engine) -> bool:
    """
    Database migration function. Checks existing DB version, runs any necessary migrations, and sets the new version
    in the metadata.

    :param engine: DB connection engine object
    """

    version = run_statement(engine, select(Metadata).where(Metadata.key == 'version')).mappings().first()
    try:
        if version:  # If the database version is set, perform migrations
            if int(version['value']) < 2:
                run_query(engine, '''
DELETE FROM planet_gasses
WHERE ROWID IN
      (
          SELECT t.ROWID FROM planet_gasses t INNER JOIN (
              SELECT *, RANK() OVER(PARTITION BY planet_id, gas_name ORDER BY id) rank
              FROM planet_gasses
          ) r ON t.id = r.id WHERE r.rank > 1
      )
                ''')
                add_column(engine, 'systems', Column('x', Float(), default=0.0))
                add_column(engine, 'systems', Column('y', Float(), default=0.0))
                add_column(engine, 'systems', Column('z', Float(), default=0.0))
                add_column(engine, 'systems', Column('region', Integer(), nullable=True))
                add_column(engine, 'stars', Column('distance', Float(), nullable=True))
                modify_table(engine, Star)
                modify_table(engine, Planet)
                modify_table(engine, PlanetGas)
            if int(version['value']) < 3:
                add_column(engine, 'systems', Column('body_count', Integer(), default=1))
                add_column(engine, 'systems', Column('non_body_count', Integer(), default=0))
                add_column(engine, 'stars', Column('subclass', Integer(), default=0))
                add_column(engine, 'stars', Column('mass', Float(), default=0.0))
                add_column(engine, 'planets', Column('mass', Float(), default=0.0))
                add_column(engine, 'planets', Column('terraform_state', String(), default=''))
                modify_table(engine, System)
                modify_table(engine, Star)
                modify_table(engine, Planet)
    except ValueError as ex:
        run_statement(engine, insert(Metadata).values(key='version', value=database_version)
                      .on_conflict_do_update(index_elements=['key'], set_=dict(value=1)))
        logger.error("An attempted fix was made for a known migration issue, please rerun EDMC", exc_info=ex)
        return False
    except Exception as ex:
        logger.error('Problem during migration', exc_info=ex)
        return False

    run_statement(engine, insert(Metadata).values(key='version', value=database_version)
                  .on_conflict_do_update(index_elements=['key'], set_=dict(value=database_version)))
    return True


def init() -> None:
    if not this.sql_engine:
        # Migrate from older BioScan DB
        old_path = config.app_dir_path / 'bioscan.db'
        engine_path = config.app_dir_path / 'explodata.db'
        if old_path.exists():
            old_path.rename(engine_path)

        # Set up engine and construct DB
        this.sql_engine = create_engine(f'sqlite:///{engine_path}', connect_args={'timeout': 30})
        Base.metadata.create_all(this.sql_engine)
        result = migrate(this.sql_engine)
        if not result:
            this.migration_failed = True
        this.sql_session_factory = scoped_session(sessionmaker(bind=this.sql_engine))


def shutdown() -> None:
    if this.journal_thread and this.journal_thread.is_alive():
        this.journal_stop = True
        if this.journal_event:
            this.journal_event.set()
    try:
        this.sql_session_factory.close()
        this.sql_engine.dispose()
    except Exception as ex:
        logger.error('Error during cleanup commit', exc_info=ex)


def get_session() -> Session:
    return this.sql_session_factory()


def get_engine() -> Engine:
    return this.sql_engine

# -*- coding: utf-8 -*-
# ExploData module plugin for EDMC
# Source: https://github.com/Silarn/EDMC-ExploData
# Licensed under the [GNU Public License (GPL)](http://www.gnu.org/licenses/gpl-2.0.html) version 2 or later.
import os
import threading
from sqlite3 import OperationalError
from typing import Optional

import sqlalchemy.exc
from sqlalchemy import ForeignKey, String, UniqueConstraint, select, Column, Float, Engine, text, Integer, \
    MetaData, Executable, Result, create_engine, event, DefaultClause
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, scoped_session, sessionmaker, Session
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.ddl import CreateTable

from .const import database_version, plugin_name

from EDMCLogging import get_plugin_logger
from config import config

logger = get_plugin_logger(plugin_name)


class This:
    """Holds globals."""

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


"""
Define the SQLAlchemy Schemas
"""


class Base(DeclarativeBase):
    pass


class Metadata(Base):
    __tablename__ = 'metadata'

    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(default='')


class JournalLog(Base):
    __tablename__ = 'journal_log'

    journal: Mapped[str] = mapped_column(String(32), primary_key=True)


class Commander(Base):
    __tablename__ = 'commanders'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(22), unique=True)


class System(Base):
    """ DB model for system data """
    __tablename__ = 'systems'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    x: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    y: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    z: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    region: Mapped[Optional[int]]
    body_count: Mapped[int] = mapped_column(default=1, server_default=text('1'))
    non_body_count: Mapped[int] = mapped_column(default=0, server_default=text('0'))

    statuses: Mapped[list['SystemStatus']] = relationship(backref='status', passive_deletes=True)
    planets: Mapped[list['Planet']] = relationship(backref='planet', passive_deletes=True)
    stars: Mapped[list['Star']] = relationship(backref='star', passive_deletes=True)
    non_bodies: Mapped[list['NonBody']] = relationship(backref='non_body', passive_deletes=True)


class SystemStatus(Base):
    __tablename__ = 'system_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id', ondelete="CASCADE"))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    honked: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    fully_scanned: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    fully_mapped: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    __table_args__ = (UniqueConstraint('system_id', 'commander_id', name='_system_commander_constraint'),
                      )


class Star(Base):
    """ DB model for star data """
    __tablename__ = 'stars'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id', ondelete="CASCADE"))
    name: Mapped[str]
    body_id: Mapped[int]
    statuses: Mapped[list['StarStatus']] = relationship(backref='status', passive_deletes=True)
    distance: Mapped[Optional[float]]
    mass: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    type: Mapped[str] = mapped_column(default='', server_default='')
    subclass: Mapped[int] = mapped_column(default=0, server_default=text('0'))
    luminosity: Mapped[str] = mapped_column(default='', server_default='')
    __table_args__ = (UniqueConstraint('system_id', 'name', 'body_id', name='_system_name_id_constraint'),
                      )


class StarStatus(Base):
    __tablename__ = 'star_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    star_id: Mapped[int] = mapped_column(ForeignKey('stars.id', ondelete="CASCADE"))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    discovered: Mapped[bool] = mapped_column(default=True, server_default=text('TRUE'))
    was_discovered: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    __table_args__ = (UniqueConstraint('star_id', 'commander_id', name='_star_commander_constraint'),
                      )


class Planet(Base):
    """ DB model for planet data """
    __tablename__ = 'planets'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id', ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(32))
    type: Mapped[str] = mapped_column(String(32), default='', server_default='')
    body_id: Mapped[int]
    atmosphere: Mapped[str] = mapped_column(String(32), default='', server_default='')
    volcanism: Mapped[Optional[str]] = mapped_column(String(32))
    distance: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    mass: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    gravity: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    temp: Mapped[Optional[float]]
    pressure: Mapped[Optional[float]]
    radius: Mapped[float] = mapped_column(default=0.0, server_default=text('0.0'))
    parent_stars: Mapped[str] = mapped_column(default='', server_default='')
    bio_signals: Mapped[int] = mapped_column(default=0, server_default=text('0'))
    materials: Mapped[str] = mapped_column(default='', server_default='')
    terraform_state: Mapped[str] = mapped_column(default='', server_default='')

    statuses: Mapped[list['PlanetStatus']] = relationship(backref='status', passive_deletes=True)
    gasses: Mapped[list['PlanetGas']] = relationship(backref='gas', passive_deletes=True)
    floras: Mapped[list['PlanetFlora']] = relationship(backref='flora', passive_deletes=True)

    __table_args__ = (UniqueConstraint('system_id', 'name', 'body_id', name='_system_name_id_constraint'),
                      )


class PlanetStatus(Base):
    __tablename__ = 'planet_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    planet_id: Mapped[int] = mapped_column(ForeignKey('planets.id', ondelete="CASCADE"))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    discovered: Mapped[bool] = mapped_column(default=True, server_default=text('TRUE'))
    was_discovered: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    mapped: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    was_mapped: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    efficient: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    __table_args__ = (UniqueConstraint('planet_id', 'commander_id', name='_planet_commander_constraint'),
                      )


class PlanetGas(Base):
    __tablename__ = 'planet_gasses'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    planet_id: Mapped[int] = mapped_column(ForeignKey('planets.id', ondelete="CASCADE"))
    gas_name: Mapped[str]
    percent: Mapped[float]
    __table_args__ = (UniqueConstraint('planet_id', 'gas_name', name='_planet_gas_constraint'), )

    def __repr__(self) -> str:
        return f'PlanetGas(gas_name={self.gas_name!r}, percent={self.percent!r})'


class PlanetFlora(Base):
    __tablename__ = 'planet_flora'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    planet_id: Mapped[int] = mapped_column(ForeignKey('planets.id', ondelete="CASCADE"))
    genus: Mapped[str]
    species: Mapped[str] = mapped_column(default='', server_default='')
    color: Mapped[str] = mapped_column(default='', server_default='')

    scans: Mapped[list['FloraScans']] = relationship(backref='scan', passive_deletes=True)
    waypoints: Mapped[list['Waypoint']] = relationship(backref='waypoint', passive_deletes=True)

    __table_args__ = (UniqueConstraint('planet_id', 'genus', name='_planet_genus_constraint'),
                      )


class FloraScans(Base):
    __tablename__ = 'flora_scans'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    flora_id: Mapped[int] = mapped_column(ForeignKey('planet_flora.id', ondelete="CASCADE"))
    count: Mapped[int] = mapped_column(default=0, server_default=text('0'))
    __table_args__ = (UniqueConstraint('commander_id', 'flora_id', name='_cmdr_flora_constraint'),
                      )


class Waypoint(Base):
    __tablename__ = 'flora_waypoints'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    flora_id: Mapped[int] = mapped_column(ForeignKey('planet_flora.id', ondelete="CASCADE"))
    type: Mapped[str] = mapped_column(default='tag', server_default='tag')
    latitude: Mapped[float]
    longitude: Mapped[float]


class NonBody(Base):
    __tablename__ = 'non_bodies'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    system_id: Mapped[int] = mapped_column(ForeignKey('systems.id', ondelete="CASCADE"))
    name: Mapped[str]
    body_id: Mapped[int]

    statuses: Mapped[list['NonBodyStatus']] = relationship(backref='status', passive_deletes=True)

    __table_args__ = (UniqueConstraint('system_id', 'name', 'body_id', name='_system_name_id_constraint'),
                      )


class NonBodyStatus(Base):
    __tablename__ = 'non_body_status'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    non_body_id: Mapped[int] = mapped_column(ForeignKey('non_bodies.id', ondelete="CASCADE"))
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    discovered: Mapped[bool] = mapped_column(default=True, server_default=text('TRUE'))
    was_discovered: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    mapped: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    was_mapped: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))
    efficient: Mapped[bool] = mapped_column(default=False, server_default=text('FALSE'))

    __table_args__ = (UniqueConstraint('non_body_id', 'commander_id', name='_nonbody_commander_constraint'),
                      )


class CodexScans(Base):
    __tablename__ = 'codex_scans'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    commander_id: Mapped[int] = mapped_column(ForeignKey('commanders.id', ondelete="CASCADE"))
    region: Mapped[int]
    biological: Mapped[str] = mapped_column(default='', server_default='')
    __table_args__ = (UniqueConstraint('commander_id', 'region', 'biological', name='_cmdr_bio_region_constraint'),)


"""
Database migration functions
"""


def modify_table(engine: Engine, table: type[Base], required_tables: Optional[list[type[Base]]] = None):
    """
    Creates a fresh copy of the target table, copies the old data into it, and replaces the old table.
    This is the only way to fully update table and column definitions in SQLite.

    :param engine: The SQLAlchemy engine
    :param table: The base class type of the table to be recreated
    :param required_tables: (Optional) A list of base class types for tables that
                            are relationship requirements of the target table
    """

    new_table_name = f'{table.__tablename__}_new'
    run_query(engine, 'PRAGMA foreign_keys=off')
    statement = text(f'DROP TABLE IF EXISTS {new_table_name}')  # drop table left over from failed migration
    run_statement(engine, statement)
    metadata = MetaData()
    if required_tables:
        for parent_table in required_tables:
            parent_table.__table__.to_metadata(metadata)
    new_table = table.__table__.to_metadata(metadata, name=new_table_name)
    column_names: list[str] = table.__table__.columns.keys()
    statement = text(str(CreateTable(new_table).compile(engine)))
    run_statement(engine, statement)
    statement = text(f'INSERT INTO `{new_table_name}` ({", ".join(column_names)}) SELECT {", ".join(column_names)} '
                     f'FROM `{table.__tablename__}`')
    run_statement(engine, statement)
    statement = text(f'DROP TABLE `{table.__tablename__}`')
    run_statement(engine, statement)
    statement = text(f'ALTER TABLE `{new_table_name}` RENAME TO `{table.__tablename__}`')
    run_statement(engine, statement)
    run_query(engine, 'PRAGMA foreign_keys=on')


def add_column(engine: Engine, table_name: str, column: Column):
    """
    Add a column to an existing table

    :param engine: The SQLAlchemy engine
    :param table_name: The name of the table to modify
    :param column: The SQLAlchemy column object to add to the table
    """

    compiler = column.compile(dialect=engine.dialect)
    column_name = str(compiler)
    column_type = column.type.compile(engine.dialect)
    null_text = ' NOT NULL' if not column.nullable else ''
    default_value = None
    if isinstance(column.server_default, DefaultClause):
        default: DefaultClause = column.server_default
        if isinstance(default.arg, str):
            default_value = compiler.render_literal_value(
                default.arg, sqltypes.STRINGTYPE
            )
        else:
            default_value = compiler.process(default.arg)
    default_text = f' DEFAULT {default_value}' if default_value is not None else ''

    try:
        statement = text(f'ALTER TABLE {table_name} DROP COLUMN {column_name}')
        run_statement(engine, statement)
    except (OperationalError, sqlalchemy.exc.OperationalError):
        pass
    statement = text(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}{null_text}{default_text}')
    run_statement(engine, statement)


def run_query(engine: Engine, query: str) -> Result:
    """
    Directly execute a query string.

    :param engine: The SQLAlchemy engine
    :param query: The string to be executed
    """

    return run_statement(engine, text(query))


def run_statement(engine: Engine, statement: Executable) -> Result:
    """
    Execute a SQLAlchemy statement. Creates a fresh connection, commits, and closes the connection.

    :param engine: The SQLAlchemy engine
    :param statement: The SQLAlchemy statement to be executed
    """

    connection = engine.connect()
    result = connection.execute(statement)
    connection.commit()
    connection.close()
    return result


def affix_schemas(engine: Engine) -> None:
    """
    Run general table migrations for the entire database structure. This should affix any changes to
    column defaults, restrictions, relationships, and other structural changes.

    :param engine: The SQLAlchemy engine
    """

    modify_table(engine, Metadata)
    modify_table(engine, JournalLog)
    modify_table(engine, Commander)
    modify_table(engine, System)
    modify_table(engine, SystemStatus, [System, Commander])
    modify_table(engine, Star, [System])
    modify_table(engine, StarStatus, [Star, Commander])
    modify_table(engine, Planet, [System])
    modify_table(engine, PlanetStatus, [Planet, Commander])
    modify_table(engine, PlanetGas, [Planet])
    modify_table(engine, PlanetFlora, [Planet])
    modify_table(engine, FloraScans, [PlanetFlora, Commander])
    modify_table(engine, Waypoint, [PlanetFlora, Commander])
    modify_table(engine, CodexScans, [Commander])
    modify_table(engine, NonBody, [System])
    modify_table(engine, NonBodyStatus, [NonBody, Commander])


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
                run_query(engine, """
DELETE FROM planet_gasses WHERE ROWID IN (
      SELECT t.ROWID FROM planet_gasses t INNER JOIN (
          SELECT *, RANK() OVER(PARTITION BY planet_id, gas_name ORDER BY id) rank FROM planet_gasses) r
      ON t.id = r.id WHERE r.rank > 1
  )
                """)
                add_column(engine, 'systems', Column('x', Float(), nullable=False, server_default=text('0.0')))
                add_column(engine, 'systems', Column('y', Float(), nullable=False, server_default=text('0.0')))
                add_column(engine, 'systems', Column('z', Float(), nullable=False, server_default=text('0.0')))
                add_column(engine, 'systems', Column('region', Integer(), nullable=True))
                add_column(engine, 'stars', Column('distance', Float(), nullable=True))
            if int(version['value']) < 3:
                run_query(engine, 'DELETE FROM journal_log')
                run_query(engine, """
DELETE FROM planet_status WHERE planet_id IN (
    SELECT p.ROWID FROM planets AS p INNER JOIN (
        SELECT *, RANK() OVER(PARTITION BY system_id, name, body_id ORDER BY id) rank FROM planets) t
    ON p.id = t.id WHERE t.rank > 1
)
                """)
                run_query(engine, """
DELETE FROM planet_flora WHERE planet_id IN (
    SELECT p.ROWID FROM planets AS p INNER JOIN (
        SELECT *, RANK() OVER(PARTITION BY system_id, name, body_id ORDER BY id) rank FROM planets) t
    ON p.id = t.id WHERE t.rank > 1
)
                """)
                run_query(engine, """
DELETE FROM planet_gasses WHERE planet_id IN (
    SELECT p.ROWID FROM planets AS p INNER JOIN (
        SELECT *, RANK() OVER(PARTITION BY system_id, name, body_id ORDER BY id) rank FROM planets) t
    ON p.id = t.id WHERE t.rank > 1
)
                """)
                run_query(engine, """
DELETE FROM planets WHERE ROWID IN (
    SELECT p.ROWID FROM planets AS p INNER JOIN (
        SELECT *, RANK() OVER(PARTITION BY system_id, name, body_id ORDER BY id) rank FROM planets) t
    ON p.id = t.id WHERE t.rank > 1
)
                """)
                run_query(engine, 'UPDATE systems SET x=0.0, y=0.0, z=0.0 WHERE x IS NULL')
                add_column(engine, 'systems', Column('body_count', Integer(), nullable=False, server_default=text('1')))
                add_column(engine, 'systems', Column('non_body_count', Integer(), nullable=False, server_default=text('0')))
                add_column(engine, 'stars', Column('subclass', Integer(), nullable=False, server_default=text('0')))
                add_column(engine, 'stars', Column('mass', Float(), nullable=False, server_default=text('0.0')))
                add_column(engine, 'planets', Column('mass', Float(), nullable=False, server_default=text('0.0')))
                add_column(engine, 'planets', Column('terraform_state', String(), nullable=False, server_default=''))
            if int(version['value']) < 4:
                run_query(engine, 'DELETE FROM journal_log')
                add_column(engine, 'planets', Column('pressure', Float(), nullable=True))
                add_column(engine, 'planets', Column('radius', Float(), nullable=False, server_default=text('0.0')))
                affix_schemas(engine)  # This should be run on the latest migration
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


"""
Database initialization
"""


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record) -> None:
    """
    Event listener to set foreign keys on for the sqlite database any time the Engine opens a connection
    """

    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def init() -> bool:
    """
    Initialize the database and run migrations (if needed)

    :return: True if a migration error occurred
    """
    if not this.sql_engine:
        # Migrate from older BioScan DB
        old_path = config.app_dir_path / 'bioscan.db'
        engine_path = config.app_dir_path / 'explodata.db'
        if old_path.exists():
            if not engine_path.exists():
                old_path.rename(engine_path)

        # Set up engine and construct DB
        this.sql_engine = create_engine(f'sqlite:///{engine_path}', connect_args={'timeout': 30})
        Base.metadata.create_all(this.sql_engine)
        result = migrate(this.sql_engine)
        if not result:
            this.migration_failed = True
        this.sql_session_factory = scoped_session(sessionmaker(bind=this.sql_engine))
    return this.migration_failed


def shutdown() -> None:
    """
    Close open sessions and dispose of the SQL engine
    """

    try:
        connect = this.sql_engine.connect()
        connect.execute(text('VACUUM'))  # Optimize size of db file
        connect.commit()
        connect.close()
        this.sql_session_factory.close()
        this.sql_engine.dispose()
    except Exception as ex:
        logger.error('Error during cleanup commit', exc_info=ex)


def get_session() -> Session:
    """
    Get a thread-safe Session for the active DB Engine

    :return: Return a new thread-safe Session object
    """

    return this.sql_session_factory()


def get_engine() -> Engine:
    """
    Get the active SQLAlchemy Engine

    :return: Return the Engine object
    """

    return this.sql_engine

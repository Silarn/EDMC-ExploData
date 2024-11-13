# ExploData

## Summary

ExploData is a python module and plugin for [Elite Dangerous Market Connector][EDMC]. It is being developed as a
database backend for the [BioScan] and [Pioneer] plugins.

While it can be run on its own, and will collect data and build a database as you play, it performs no user-facing
functions by itself.

## Data Collection

The data currently being collected by this plugin includes:

- Commanders (for data segmentation)
- Systems (name, location, body count and scan status)
- Stars (name, location, and various properties, discovery status)
- Planets (name, location, attributes, local biological signals, discovery and mapped status)
- Asteroid belts and rings (stellar and planetary)
- Non-body Objects (asteroid clusters)
- Flora (type, location, scan status)
- Codex Entries (for biological signals)

### Location

A `explodata.db` file is saved to the primary EDMC data storage directory, where plugins are installed.

On Windows this is generally located at `%LOCALAPPDATA%\EDMarketConnector\explodata.db`.

### Migrations

Simply running a new version of the plugin should automatically perform database migrations on existing data.

Plugins should check for compatible database versions.

## Journal Importing

The plugin contains a threaded journal importing process, though this must be initiated by another plugin. It will
track completed journal files and can be started and stopped on demand. Hook functions are used to initialize the
parse, track progress, and notify (and trigger data / display updates) once complete.

## EDSM Parsing

ExploData supports parsing data from EDSM. Hook functions must be set up to trigger the parse and notify plugins
which make use of the data.

## Installation

Installation instructions will generally be provided by plugins that use ExploData. It should be installed alongside
the companion plugins and must be named `ExploData` or you may run into import errors.

[EDMC]: https://github.com/EDCD/EDMarketConnector/wiki
[BioScan]: https://github.com/Silarn/EDMC-BioScan
[Pioneer]: https://github.com/Silarn/EDMC-Pioneer
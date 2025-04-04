# dialects/mysql/types.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors


import datetime

from ... import exc
from ... import util
from ...sql import sqltypes


class _NumericType:
    """Base for MySQL numeric types.

    This is the base both for NUMERIC as well as INTEGER, hence
    it's a mixin.

    """

    def __init__(self, unsigned=False, zerofill=False, **kw):
        self.unsigned = unsigned
        self.zerofill = zerofill
        super().__init__(**kw)

    def __repr__(self):
        return util.generic_repr(
            self, to_inspect=[_NumericType, sqltypes.Numeric]
        )


class _FloatType(_NumericType, sqltypes.Float):
    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        if isinstance(self, (REAL, DOUBLE)) and (
            (precision is None and scale is not None)
            or (precision is not None and scale is None)
        ):
            raise exc.ArgumentError(
                "You must specify both precision and scale or omit "
                "both altogether."
            )
        super().__init__(precision=precision, asdecimal=asdecimal, **kw)
        self.scale = scale

    def __repr__(self):
        return util.generic_repr(
            self, to_inspect=[_FloatType, _NumericType, sqltypes.Float]
        )


class _IntegerType(_NumericType, sqltypes.Integer):
    def __init__(self, display_width=None, **kw):
        self.display_width = display_width
        super().__init__(**kw)

    def __repr__(self):
        return util.generic_repr(
            self, to_inspect=[_IntegerType, _NumericType, sqltypes.Integer]
        )


class _StringType(sqltypes.String):
    """Base for MySQL string types."""

    def __init__(
        self,
        charset=None,
        collation=None,
        ascii=False,  # noqa
        binary=False,
        unicode=False,
        national=False,
        **kw,
    ):
        self.charset = charset

        # allow collate= or collation=
        kw.setdefault("collation", kw.pop("collate", collation))

        self.ascii = ascii
        self.unicode = unicode
        self.binary = binary
        self.national = national
        super().__init__(**kw)

    def __repr__(self):
        return util.generic_repr(
            self, to_inspect=[_StringType, sqltypes.String]
        )


class _MatchType(sqltypes.Float, sqltypes.MatchType):
    def __init__(self, **kw):
        # TODO: float arguments?
        sqltypes.Float.__init__(self)
        sqltypes.MatchType.__init__(self)


class NUMERIC(_NumericType, sqltypes.NUMERIC):
    """MySQL NUMERIC type."""

    __visit_name__ = "NUMERIC"

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a NUMERIC.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(
            precision=precision, scale=scale, asdecimal=asdecimal, **kw
        )


class DECIMAL(_NumericType, sqltypes.DECIMAL):
    """MySQL DECIMAL type."""

    __visit_name__ = "DECIMAL"

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a DECIMAL.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(
            precision=precision, scale=scale, asdecimal=asdecimal, **kw
        )


class DOUBLE(_FloatType, sqltypes.DOUBLE):
    """MySQL DOUBLE type."""

    __visit_name__ = "DOUBLE"

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a DOUBLE.

        .. note::

            The :class:`.DOUBLE` type by default converts from float
            to Decimal, using a truncation that defaults to 10 digits.
            Specify either ``scale=n`` or ``decimal_return_scale=n`` in order
            to change this scale, or ``asdecimal=False`` to return values
            directly as Python floating points.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(
            precision=precision, scale=scale, asdecimal=asdecimal, **kw
        )


class REAL(_FloatType, sqltypes.REAL):
    """MySQL REAL type."""

    __visit_name__ = "REAL"

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a REAL.

        .. note::

            The :class:`.REAL` type by default converts from float
            to Decimal, using a truncation that defaults to 10 digits.
            Specify either ``scale=n`` or ``decimal_return_scale=n`` in order
            to change this scale, or ``asdecimal=False`` to return values
            directly as Python floating points.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(
            precision=precision, scale=scale, asdecimal=asdecimal, **kw
        )


class FLOAT(_FloatType, sqltypes.FLOAT):
    """MySQL FLOAT type."""

    __visit_name__ = "FLOAT"

    def __init__(self, precision=None, scale=None, asdecimal=False, **kw):
        """Construct a FLOAT.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(
            precision=precision, scale=scale, asdecimal=asdecimal, **kw
        )

    def bind_processor(self, dialect):
        return None


class INTEGER(_IntegerType, sqltypes.INTEGER):
    """MySQL INTEGER type."""

    __visit_name__ = "INTEGER"

    def __init__(self, display_width=None, **kw):
        """Construct an INTEGER.

        :param display_width: Optional, maximum display width for this number.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(display_width=display_width, **kw)


class BIGINT(_IntegerType, sqltypes.BIGINT):
    """MySQL BIGINTEGER type."""

    __visit_name__ = "BIGINT"

    def __init__(self, display_width=None, **kw):
        """Construct a BIGINTEGER.

        :param display_width: Optional, maximum display width for this number.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(display_width=display_width, **kw)


class MEDIUMINT(_IntegerType):
    """MySQL MEDIUMINTEGER type."""

    __visit_name__ = "MEDIUMINT"

    def __init__(self, display_width=None, **kw):
        """Construct a MEDIUMINTEGER

        :param display_width: Optional, maximum display width for this number.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(display_width=display_width, **kw)


class TINYINT(_IntegerType):
    """MySQL TINYINT type."""

    __visit_name__ = "TINYINT"

    def __init__(self, display_width=None, **kw):
        """Construct a TINYINT.

        :param display_width: Optional, maximum display width for this number.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(display_width=display_width, **kw)


class SMALLINT(_IntegerType, sqltypes.SMALLINT):
    """MySQL SMALLINTEGER type."""

    __visit_name__ = "SMALLINT"

    def __init__(self, display_width=None, **kw):
        """Construct a SMALLINTEGER.

        :param display_width: Optional, maximum display width for this number.

        :param unsigned: a boolean, optional.

        :param zerofill: Optional. If true, values will be stored as strings
          left-padded with zeros. Note that this does not effect the values
          returned by the underlying database API, which continue to be
          numeric.

        """
        super().__init__(display_width=display_width, **kw)


class BIT(sqltypes.TypeEngine):
    """MySQL BIT type.

    This type is for MySQL 5.0.3 or greater for MyISAM, and 5.0.5 or greater
    for MyISAM, MEMORY, InnoDB and BDB.  For older versions, use a
    MSTinyInteger() type.

    """

    __visit_name__ = "BIT"

    def __init__(self, length=None):
        """Construct a BIT.

        :param length: Optional, number of bits.

        """
        self.length = length

    def result_processor(self, dialect, coltype):
        """Convert a MySQL's 64 bit, variable length binary string to a long.

        TODO: this is MySQL-db, pyodbc specific.  OurSQL and mysqlconnector
        already do this, so this logic should be moved to those dialects.

        """

        def process(value):
            if value is not None:
                v = 0
                for i in value:
                    if not isinstance(i, int):
                        i = ord(i)  # convert byte to int on Python 2
                    v = v << 8 | i
                return v
            return value

        return process


class TIME(sqltypes.TIME):
    """MySQL TIME type."""

    __visit_name__ = "TIME"

    def __init__(self, timezone=False, fsp=None):
        """Construct a MySQL TIME type.

        :param timezone: not used by the MySQL dialect.
        :param fsp: fractional seconds precision value.
         MySQL 5.6 supports storage of fractional seconds;
         this parameter will be used when emitting DDL
         for the TIME type.

         .. note::

            DBAPI driver support for fractional seconds may
            be limited; current support includes
            MySQL Connector/Python.

        """
        super().__init__(timezone=timezone)
        self.fsp = fsp

    def result_processor(self, dialect, coltype):
        time = datetime.time

        def process(value):
            # convert from a timedelta value
            if value is not None:
                microseconds = value.microseconds
                seconds = value.seconds
                minutes = seconds // 60
                return time(
                    minutes // 60,
                    minutes % 60,
                    seconds - minutes * 60,
                    microsecond=microseconds,
                )
            else:
                return None

        return process


class TIMESTAMP(sqltypes.TIMESTAMP):
    """MySQL TIMESTAMP type."""

    __visit_name__ = "TIMESTAMP"

    def __init__(self, timezone=False, fsp=None):
        """Construct a MySQL TIMESTAMP type.

        :param timezone: not used by the MySQL dialect.
        :param fsp: fractional seconds precision value.
         MySQL 5.6.4 supports storage of fractional seconds;
         this parameter will be used when emitting DDL
         for the TIMESTAMP type.

         .. note::

            DBAPI driver support for fractional seconds may
            be limited; current support includes
            MySQL Connector/Python.

        """
        super().__init__(timezone=timezone)
        self.fsp = fsp


class DATETIME(sqltypes.DATETIME):
    """MySQL DATETIME type."""

    __visit_name__ = "DATETIME"

    def __init__(self, timezone=False, fsp=None):
        """Construct a MySQL DATETIME type.

        :param timezone: not used by the MySQL dialect.
        :param fsp: fractional seconds precision value.
         MySQL 5.6.4 supports storage of fractional seconds;
         this parameter will be used when emitting DDL
         for the DATETIME type.

         .. note::

            DBAPI driver support for fractional seconds may
            be limited; current support includes
            MySQL Connector/Python.

        """
        super().__init__(timezone=timezone)
        self.fsp = fsp


class YEAR(sqltypes.TypeEngine):
    """MySQL YEAR type, for single byte storage of years 1901-2155."""

    __visit_name__ = "YEAR"

    def __init__(self, display_width=None):
        self.display_width = display_width


class TEXT(_StringType, sqltypes.TEXT):
    """MySQL TEXT type, for character storage encoded up to 2^16 bytes."""

    __visit_name__ = "TEXT"

    def __init__(self, length=None, **kw):
        """Construct a TEXT.

        :param length: Optional, if provided the server may optimize storage
          by substituting the smallest TEXT type sufficient to store
          ``length`` bytes of characters.

        :param charset: Optional, a column-level character set for this string
          value.  Takes precedence to 'ascii' or 'unicode' short-hand.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param ascii: Defaults to False: short-hand for the ``latin1``
          character set, generates ASCII in schema.

        :param unicode: Defaults to False: short-hand for the ``ucs2``
          character set, generates UNICODE in schema.

        :param national: Optional. If true, use the server's configured
          national character set.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super().__init__(length=length, **kw)


class TINYTEXT(_StringType):
    """MySQL TINYTEXT type, for character storage encoded up to 2^8 bytes."""

    __visit_name__ = "TINYTEXT"

    def __init__(self, **kwargs):
        """Construct a TINYTEXT.

        :param charset: Optional, a column-level character set for this string
          value.  Takes precedence to 'ascii' or 'unicode' short-hand.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param ascii: Defaults to False: short-hand for the ``latin1``
          character set, generates ASCII in schema.

        :param unicode: Defaults to False: short-hand for the ``ucs2``
          character set, generates UNICODE in schema.

        :param national: Optional. If true, use the server's configured
          national character set.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super().__init__(**kwargs)


class MEDIUMTEXT(_StringType):
    """MySQL MEDIUMTEXT type, for character storage encoded up
    to 2^24 bytes."""

    __visit_name__ = "MEDIUMTEXT"

    def __init__(self, **kwargs):
        """Construct a MEDIUMTEXT.

        :param charset: Optional, a column-level character set for this string
          value.  Takes precedence to 'ascii' or 'unicode' short-hand.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param ascii: Defaults to False: short-hand for the ``latin1``
          character set, generates ASCII in schema.

        :param unicode: Defaults to False: short-hand for the ``ucs2``
          character set, generates UNICODE in schema.

        :param national: Optional. If true, use the server's configured
          national character set.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super().__init__(**kwargs)


class LONGTEXT(_StringType):
    """MySQL LONGTEXT type, for character storage encoded up to 2^32 bytes."""

    __visit_name__ = "LONGTEXT"

    def __init__(self, **kwargs):
        """Construct a LONGTEXT.

        :param charset: Optional, a column-level character set for this string
          value.  Takes precedence to 'ascii' or 'unicode' short-hand.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param ascii: Defaults to False: short-hand for the ``latin1``
          character set, generates ASCII in schema.

        :param unicode: Defaults to False: short-hand for the ``ucs2``
          character set, generates UNICODE in schema.

        :param national: Optional. If true, use the server's configured
          national character set.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super().__init__(**kwargs)


class VARCHAR(_StringType, sqltypes.VARCHAR):
    """MySQL VARCHAR type, for variable-length character data."""

    __visit_name__ = "VARCHAR"

    def __init__(self, length=None, **kwargs):
        """Construct a VARCHAR.

        :param charset: Optional, a column-level character set for this string
          value.  Takes precedence to 'ascii' or 'unicode' short-hand.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param ascii: Defaults to False: short-hand for the ``latin1``
          character set, generates ASCII in schema.

        :param unicode: Defaults to False: short-hand for the ``ucs2``
          character set, generates UNICODE in schema.

        :param national: Optional. If true, use the server's configured
          national character set.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super().__init__(length=length, **kwargs)


class CHAR(_StringType, sqltypes.CHAR):
    """MySQL CHAR type, for fixed-length character data."""

    __visit_name__ = "CHAR"

    def __init__(self, length=None, **kwargs):
        """Construct a CHAR.

        :param length: Maximum data length, in characters.

        :param binary: Optional, use the default binary collation for the
          national character set.  This does not affect the type of data
          stored, use a BINARY type for binary data.

        :param collation: Optional, request a particular collation.  Must be
          compatible with the national character set.

        """
        super().__init__(length=length, **kwargs)

    @classmethod
    def _adapt_string_for_cast(cls, type_):
        # copy the given string type into a CHAR
        # for the purposes of rendering a CAST expression
        type_ = sqltypes.to_instance(type_)
        if isinstance(type_, sqltypes.CHAR):
            return type_
        elif isinstance(type_, _StringType):
            return CHAR(
                length=type_.length,
                charset=type_.charset,
                collation=type_.collation,
                ascii=type_.ascii,
                binary=type_.binary,
                unicode=type_.unicode,
                national=False,  # not supported in CAST
            )
        else:
            return CHAR(length=type_.length)


class NVARCHAR(_StringType, sqltypes.NVARCHAR):
    """MySQL NVARCHAR type.

    For variable-length character data in the server's configured national
    character set.
    """

    __visit_name__ = "NVARCHAR"

    def __init__(self, length=None, **kwargs):
        """Construct an NVARCHAR.

        :param length: Maximum data length, in characters.

        :param binary: Optional, use the default binary collation for the
          national character set.  This does not affect the type of data
          stored, use a BINARY type for binary data.

        :param collation: Optional, request a particular collation.  Must be
          compatible with the national character set.

        """
        kwargs["national"] = True
        super().__init__(length=length, **kwargs)


class NCHAR(_StringType, sqltypes.NCHAR):
    """MySQL NCHAR type.

    For fixed-length character data in the server's configured national
    character set.
    """

    __visit_name__ = "NCHAR"

    def __init__(self, length=None, **kwargs):
        """Construct an NCHAR.

        :param length: Maximum data length, in characters.

        :param binary: Optional, use the default binary collation for the
          national character set.  This does not affect the type of data
          stored, use a BINARY type for binary data.

        :param collation: Optional, request a particular collation.  Must be
          compatible with the national character set.

        """
        kwargs["national"] = True
        super().__init__(length=length, **kwargs)


class TINYBLOB(sqltypes._Binary):
    """MySQL TINYBLOB type, for binary data up to 2^8 bytes."""

    __visit_name__ = "TINYBLOB"


class MEDIUMBLOB(sqltypes._Binary):
    """MySQL MEDIUMBLOB type, for binary data up to 2^24 bytes."""

    __visit_name__ = "MEDIUMBLOB"


class LONGBLOB(sqltypes._Binary):
    """MySQL LONGBLOB type, for binary data up to 2^32 bytes."""

    __visit_name__ = "LONGBLOB"

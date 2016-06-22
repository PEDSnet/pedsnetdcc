from abc import ABCMeta, abstractmethod

from pedsnetdcc import VOCAB_TABLES


class Transform(object):
    """Abstract class to define an interface for transforming the CDM.

    The PEDSNet DCC makes custom modifications to the PEDSNet Common
    Data Model for performance and other reasons.

    For practical reasons, the transformations are effected by
    creating a temporary table via a `CREATE TABLE foo_tmp AS SELECT
    ... FROM foo ...` statement, where the SELECT may add additional
    columns to `foo` (and include any joins necessary to populating
    the additional columns).

    Subclasses of the Transform class help to create these SELECT
    statements and are also able produce any required index creation
    statements.
    """
    __metaclass__ = ABCMeta

    @classmethod
    def pre_transform(cls, dburi, schema):
        """Execute statements required before a transform.

        A Transform can override this method to execute prerequisite
        statements for a transform, e.g. creating database functions, etc.

        :param str dburi:
        :param str schema:
        :return: None
        """
        pass

    @classmethod
    @abstractmethod
    def modify_select(cls, metadata, table_name, select, join):
        """Transform a Select object into a new Select object.

        Also transform a corresponding Join object by chaining any
        additional Join objects to it.

        The return value is a (Select, Join) tuple that may be passed
        to another Transform's `modify_select` method or combined into
        a final, complete Select object via
        `final_select.select_from(final_join)`.

        The initial `select` object fed to the first of a chain of
        transformations should represent a plain select of all columns
        on the desired table from the data model, e.g. `select([a_table])`.
        The initial `join` object should be simply `a_table`.

        A transformation can operate on this statement object to add
        column expressions. If there is a need to remove columns, this API
        should probably be changed.

        Corresponding to these select modifications, any required
        joins should be chained to the `join` object.

        Any transform-specific arguments can be passed via the
        `metadata.info` dict.

        :param sqlalchemy.schema.MetaData metadata: object describing tables
        and columns
        :param str table_name: Name of table to transform
        :param sqlalchemy.sql.expression.Select select: SQLAlchemy Select
        statement to transform
        :param sqlalchemy.sql.expression.Join join:
        :rtype: sqlalchemy.sql.expression.Select,
        sqlalchemy.sql.expression.Join

        """
        pass

    @classmethod
    def modify_metadata(cls, metadata):
        """Modify SQLAlchemy metadata for all appropriate tables.

        Iterate over all non-vocabulary tables and run `modify_table`.

        The only current use of this is to allow the user to iterate over
        modified tables and generate indexes and constraints.

        :param sqlalchemy.MetaData metadata: SQLAlchemy Metadata object
        describing tables and columns
        :rtype: sqlalchemy.MetaData

        """
        for table in metadata.tables.values():
            if table in VOCAB_TABLES:
                continue

            cls.modify_table(metadata, table)

        return metadata

    @classmethod
    @abstractmethod
    def modify_table(cls, metadata, table):
        """Helper function to apply the transformation to a table in place.

        The user must implement this method.

        :param sqlalchemy.schema.MetaData metadata:
        :param sqlalchemy.schema.Table table:
        :return: None
        """
        pass

    # TODO: we could define a non-abstract method to execute a bunch
    # of transformations.

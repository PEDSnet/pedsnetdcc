from abc import ABCMeta, abstractmethod


class Transform(object):
    """Abstract class to define an interface for generating transformations

    The primary transformation, via the `modify_select` method, takes
    a SQLAlchemy Select object and adds columns to it of the correct
    type and name (alias).
    """
    __metaclass__ = ABCMeta

    @classmethod
    @abstractmethod
    def modify_select(cls, metadata, table_name, select):
        """Transform a Select object into a new Select object

        The Select statement object for an untransformed table is a
        representation of "select col1, ... from table1".  A
        transformation can operate on this statement object to add,
        remove, or modify select expressions, add joins, etc.

        Any transform-specific data can be shoehorned into
        a `metadata.info` dict.

        :param Metadata metadata: SQLAlchemy Metadata object describing
        tables and columns
        :param str table_name: Name of table to transform
        :param Select select: SQLAlchemy Select statement to transform
        :rtype: Select

        """
        pass

    @classmethod
    @abstractmethod
    def modify_metadata(cls, metadata):
        """Modify SQLAlchemy metadata for all appropriate tables.

        The only current use of this is to allow the user to iterate over
        modified tables and generate indexes and constraints.

        :param sqlalchemy.MetaData metadata: SQLAlchemy Metadata object
        describing tables and columns
        :rtype: sqlalchemy.MetaData

        """

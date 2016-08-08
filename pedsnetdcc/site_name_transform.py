import logging
from sqlalchemy import literal_column, String
from sqlalchemy.schema import Column, Index

from pedsnetdcc.abstract_transform import Transform

logger = logging.getLogger(__name__)


class SiteNameTransform(Transform):

    @classmethod
    def modify_select(cls, metadata, table_name, select, join):
        """Add a site name column to the table.

        The text value of the column is drawn from the `metadata.info` dict.
        """

        if 'site' not in metadata.info:
            logger.critical({
                'msg': 'metadata.info dict should have `site` entry'
            })
            raise RuntimeError

        new_col = literal_column("'{0}'::varchar(32)".
                                 format(metadata.info['site'])).label('site')
        select = select.column(new_col)

        return select, join

    @classmethod
    def modify_table(cls, metadata, table):
        """Helper function to apply the transformation to a table in place.
        See Transform.modify_table for signature.
        """

        new_col = Column('site', String)
        table.append_column(new_col)
        Index(Transform.make_index_name(table.name, 'site'), new_col)

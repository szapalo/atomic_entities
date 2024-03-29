"""
Valid SQLite URL forms are:
 sqlite:///:memory: (or, sqlite://)
 sqlite:///relative/path/to/file.db
 sqlite:////absolute/path/to/file.db

"""
import signal # unlike 'aexit', it can run exit handles on termination/kill/error
# use 'signal' to disconnect

import typing
from sqlalchemy import (
    create_engine, Engine, MetaData, Table, Column, MappingResult,
    Integer, String, select, update, insert, delete
)

from . import base_factory
from ..api import sql_entity
from ..utils import BinaryExpression, Field, BoolOperators


MANDATORY_CONFIG_ENTRIES = [
    "table_name", "base_fields",
]

class SQLEntityFactory(base_factory.EntityFactory):
    def __init__(
            self, entity_name: str, config: dict,
            entities_factory: base_factory.Factory, table_name: str
    ) -> None:
        super().__init__(entity_name, config, entities_factory)
        
        self.table = entities_factory.metadata.tables[table_name]
        self.engine = entities_factory.engine

    # def get_all_fields(self):
    #     return [
    #         self.talbe.c[i] for i in self.table
    #     ]

    def _build_api(self):
        class APIClass(sql_entity.SQLAPI):
            pass
        
        APIClass._table_ = self.table
        APIClass._engine_ = self.engine
        APIClass._conn = self.engine.connect()
        APIClass._select_table = select(self.table)
        APIClass._update_table = update(self.table)
        APIClass._insert_table = insert(self.table)
        APIClass._select_base_fields = select(*[
            self.table.c[f] for f in self.get_base_keys()
        ])

        self.entity_cls._DS_API =  APIClass

    def _build_collection_cls(self):
        super()._build_collection_cls()
        # self.collection_cls._DSC_API = sql_entity.SQLCollectionAPI

    def _build_entity_cls(self):
        super()._build_entity_cls()
        self._build_api()
        self.entity_cls._all_keys = self.table.columns.keys()

    def build_entity(self):
        result = super().build_entity()
        # self._build_api()
        return result



class Factory(base_factory.Factory):

    _entity_factory_cls = SQLEntityFactory

    def __init__(self, config) -> None:
        super().__init__(config)

        db_path = config['path']
        
        self.engine = create_engine(db_path)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.table_names = self.metadata.tables.keys()


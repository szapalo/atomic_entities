
from pymongo import MongoClient

from . import base_factory
from ..api import mongo_entity


    
class MongoEntityFactory(base_factory.EntityFactory):
    def __init__(self, entity_name: str, config: dict, entities_factory: "Factory", collection_name: str) -> None:
        super().__init__(entity_name, config, entities_factory)
        
        self.collection = entities_factory.database.get_collection(collection_name)

    def _build_api(self):
        class APIClass(mongo_entity.MongoAPI):
            pass
        
        APIClass._collection = self.collection
        self.entity_cls._DS_API =  APIClass

    # def _build_collection_cls(self):
    #     super()._build_collection_cls()
        # self.collection_cls._DSC_API = sql_entity.SQLCollectionAPI
    
    def _build_entity_cls(self):
        super()._build_entity_cls()
        self._build_api()
        self.entity_cls._all_keys = self.collection.keys()


class Factory():

    _entity_factory_cls = MongoEntityFactory
    _table_lable = "collection"

    def __init__(self, config):
        super().__init__(config)

        db_path = config['path']
        self.client = MongoClient(db_path)

        database_name = config.get('database')
        if not self.database in self.client.list_database_names():
            raise Exception('Database "{}" in {} does not exist'.format(
                self.database, db_path
            ))
        
        self.database = self.client.get_database(database_name)
        # collection_name = config.get('table_name')
        # self.collection_names = self.database.list_collection_names()


import os
import json
import yaml

from .factories import sql_factory

SQL_SUPPORT = ["SQL","MySQL", "Oracle", "PostgreSQL", "SQLite"]
MONGODB_SUPPORT = ["MongoDB"]
DYNAMODB_SUPPORT = ["DynamoDB"]
SHOTGRID_SUPPORT = ["SG", "ShotGrid"]


DS_SUPPORT = SQL_SUPPORT # + MONGODB_SUPPORT + DYNAMODB_SUPPORT + SHOTGRID_SUPPORT


_supported_databases_err = Exception(
    'Supported "datasource" config directive only allows one of the following: {}'.format(
        ', '.join('"{0}"'.format(db) for db in DS_SUPPORT)
    )
)

_config_input_err = Exception(
    "Config arg to Factory must be a list[dict], or a valid path to a json or yaml file."
)


class Factory:
    def __init__(self, config=None):
        self.ds_factories = []
        self.entities_map = {}
        self.collections_map = {}
        if not config:
            config = os.environ['ATOMIC_ENTITIES_CONFIG']
        if isinstance(config,str):
            ext = os.path.splitext(config)[-1]
            with open(config) as f:
                match ext:
                    case ".json":    
                        self.config = json.load(f)
                    case ".yaml" | ".yml":
                        self.config = yaml.load(f)
                    case _:
                        raise _config_input_err
        elif (
            isinstance(config, list) and
            len(config) and 
            isinstance(config[0], dict)
        ):
            self.config = config
        else:
            raise _config_input_err


    def build(self):
        for conf_ds in self.config:
            ds_type = conf_ds["datasource"]
            match ds_type :
                case ds if ds in SQL_SUPPORT:
                    self.ds_factories.append(sql_factory.Factory(conf_ds))
                # case ds if ds in MONGODB_SUPPORT:
                #     self.ds_factories.append(MongoBase.Factory(conf_ds))
                # case ds if ds in DYNAMO_SUPPORT:
                    # self.ds_factories.append(DynamoBase.Factory(conf_ds))
                # case ds if ds in SHOTGRID_SUPPORT:
                    # self.ds_factories.append(ShotGridBase.Factory(conf_ds))
                case _ :
                    raise _supported_databases_err
        
        for ds_factory in self.ds_factories:
            ds_factory.build_entities()
            self.entities_map.update(ds_factory.entities_map)
            self.collections_map.update(ds_factory.collections_map)

        for ds_factory in self.ds_factories:
            ds_factory.crosslink_entities(self.entities_map)
    
    def get_entities(self):
        return [
            entity for ds_factory in self.ds_factories
            for entity in ds_factory.get_entities()
        ]
    def get_entity(self, entity_name):
        return 



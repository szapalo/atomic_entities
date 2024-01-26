
import os
import json
import yaml

from .factories import sql_factory, mongo_factory

SQL_SUPPORT = ["SQL","MySQL", "Oracle", "PostgreSQL", "SQLite"]
MONGODB_SUPPORT = ["MongoDB"]
DYNAMODB_SUPPORT = ["DynamoDB"]
SHOTGRID_SUPPORT = ["SG", "ShotGrid"]

DS_SUPPORT = SQL_SUPPORT + MONGODB_SUPPORT # + DYNAMODB_SUPPORT + SHOTGRID_SUPPORT
ENV_CONFIG = "ATOMIC_ENTITIES_CONFIG" 
LOCAL_CONFIG_BASENAME = "atomic_entities_config"

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
            env_path = os.getcwd()
            local_config_paths = [
                f for f in os.listdir(env_path) if os.path.isfile(f) and
                os.path.splitext(f)[0] == LOCAL_CONFIG_BASENAME
            ]
        
            if local_config_paths:
                config = local_config_paths[0]
            else:
                config = os.environ[ENV_CONFIG]

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


    def _validate_entity_names(self):
        entity_names_set = set()
        for conf_ds in self.config:
            entity_names = conf_ds['entities'].keys()
            name_duplicates = entity_names_set.intersection(entity_names)
            if name_duplicates:
                raise Exception(
                    "Entity names must be unique. \
                     Duplicates found in config: {}.".format(name_duplicates)
                )
            entity_names_set.union(entity_names)

    def build(self):
        for conf_ds in self.config:
            ds_type = conf_ds["datasource"]
            match ds_type:
                case ds if ds in SQL_SUPPORT:
                    self.ds_factories.append(sql_factory.Factory(conf_ds))
                case ds if ds in MONGODB_SUPPORT:
                    self.ds_factories.append(mongo_factory.Factory(conf_ds))
                # case ds if ds in DYNAMO_SUPPORT:
                    # self.ds_factories.append(DynamoBase.Factory(conf_ds))
                # case ds if ds in SHOTGRID_SUPPORT:
                    # self.ds_factories.append(ShotGridBase.Factory(conf_ds))
                case _ :
                    # pass
                    raise _supported_databases_err
        
        for ds_factory in self.ds_factories:
            ds_factory.build_entities()
            self.entities_map.update(ds_factory.entities_map)
            self.collections_map.update(ds_factory.collections_map)

        for ds_factory in self.ds_factories:
            ds_factory.crosslink_entities(self.entities_map)
    
    def fetch_entities(self):
        self.entities_map = {
            entity.__name__ : entity for ds_factory in self.ds_factories
            for entity in ds_factory.get_entities()
        }

    def get_entity(self, entity_name):
        return 



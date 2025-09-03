
import typing
from ..api import base_entity
from ..utils import Field
_MANDATORY_CONFIG_SPECS = [ 'datasource', 'path' ]
_MANDATORY_ENTITY_CONFIG_SPECS = [ 'table_name', 'base_fields' ]
_MANDATORY_PROPERTIES_CONFIG_SPECS = []
_MANDATORY_LINKPROPS_CONFIG_SPECS = [ 'target_entity', 'source_field' ]
_DISALLOWLIST_PROP_SPECS = dir(base_entity.BaseEntity)

_ITER_TYPES = (list, tuple)

class Factory:

    # Initialized by derived Factory
    _entity_factory_cls = None
    _table_lable = "table"

    @classmethod
    def _validate_config(cls, config: dict):
        # TODO
        return True

    def __init__(self, config: dict):
        self._validate_config(config)
        self.config = config
        self.entities_map = {}
        self.collections_map = {}
        self.entity_factories = None


    def init_entity_factories(self) -> "EntityFactory":
        print("init_entity_factories")
        config_entities = self.config['entities']
        entity_factories = []
        for entity_name, entity_config in config_entities.items():
            table_name = entity_config.get("table_name")
            if table_name in self.table_names:
                entity_factory = self._entity_factory_cls(
                    entity_name,
                    entity_config,
                    self,
                    table_name
                )
                entity_factories.append(entity_factory)
            elif table_name:
                raise Exception(
                    '{} "{}" is not found in datasource'.format(
                        self._table_lable, table_name
                    )
                )
            else:
                raise Exception(
                    'Config does not specify "table_name" \
                    for entity "{}"'.format(entity_name)
                )
        print("entity_factories ={}".format(entity_factories))
        return entity_factories

    def build_entities(self):
        self.entity_factories = self.init_entity_factories()
        for entity_factory in self.entity_factories:
            (
                self.entities_map[entity_factory.name],
                self.collections_map[entity_factory.name]
            ) = entity_factory.build_entity()
    
    def crosslink_entities(
            self, entities_map: typing.Mapping[str, base_entity.BaseEntity]
    ):
        for entity_factory in self.entity_factories:
            entity_factory.crosslink_properties(entities_map)

    def build(self):
        self.build_entities()
        self.crosslink_entities()

class EntityFactory:
    @classmethod
    def _validate_config(cls, config):
        return True

    def __init__(self, name: str, entity_config: dict, factory: Factory):
        self.name = name
        self.config = entity_config
        self._entities_factory = factory
        self.entity_cls = None

    def get_entities(self):
        return [
            entity_factory.get_entity()
            for entity_factory in self.entity_factories
        ]

    def get_entity(self, entity_name):
        return self.entities_map[entity_name]

    @classmethod
    def _get_link_method(cls, tgt_cls: base_entity.BaseEntity,
                         tgt_field: Field, src_key: typing.Any, limit: int):
        match limit:
            case 1 :
                def link_method(self):
                    return tgt_cls.findOne(tgt_field == self[src_key])
            case None:
                def link_method(self):
                    value = self[src_key]
                    if not isinstance(value, _ITER_TYPES): value = [value]
                    print(value)
                    return tgt_cls.find(tgt_field.in_(value))
            case _:
                def link_method(self):
                    value = self[src_key]
                    if not isinstance(value, _ITER_TYPES): value = [value]
                    return tgt_cls.find(
                        tgt_field.in_(value), _limit=limit
                    )
        
        return property(fget=link_method)

    def get_base_keys(self):
        return self.config['base_fields']

    def _build_entity_cls(self):
        base_fields = self.get_base_keys()
        class EntityClass(base_entity.BaseEntity):
            pass
        
        self.entity_cls = EntityClass

        EntityClass.__name__ = EntityClass.__qualname__ = self.name
        EntityClass._base_fields = base_fields
        EntityClass._primary_key = Field(self.entity_cls,
                                         self.config["primary_key"])

    def _set_field_map(self):
        self.entity_cls.field_map = {
            key: Field(self.entity_cls, key) 
            for key in self.entity_cls._all_keys
        }

    def _build_collection_cls(self):
        
        class EntityCollection(base_entity.BaseCollection):
            pass

        EntityCollection.__name__ = EntityCollection.__qualname__ = \
            "{}Collection".format(self.name)

        self.collection_cls = EntityCollection

    def _link_collection_and_entity(self):
        self.collection_cls._entity_cls = self.entity_cls
        self.entity_cls._collection_cls = self.collection_cls

    def _build_property(self, prop_name, key):

            print("Set prop : {} -> {}".format(prop_name, key) )
            prop_method = property(
                fget = lambda self: self._data[key],
                fset = lambda self,v: self._data.__setitem__(key, v)
            )
            setattr(self.entity_cls, prop_name, prop_method)

    def _build_properties(self):        
        prop_config = self.config['properties']
        self.entity_cls._mapper = prop_config
        
        for prop_name, name in prop_config.items():
            self._build_property(prop_name, name)

    def _crosslink_property(self, target_entity_cls, prop_name, prop_config):

        source_key_name = prop_config['source_field']
        limit = prop_config.get('limit', 1)

        target_key_name = prop_config.get('target_field')
        if not target_key_name:
            # TODO : default fallback should be target_entity's primary key 
            raise Exception(
                "target_key_name is not specified \
                in config's entity {}.".format(self.name)
            )

        setattr(
            self.entity_cls,
            prop_name,
            self._get_link_method(
                target_entity_cls, 
                Field(target_entity_cls, target_key_name), 
                source_key_name, limit
            )
        )

    def crosslink_properties(
            self, entities_map :typing.Mapping[str, base_entity.BaseEntity]
    ):
        
        link_config = self.config.get('link_properties')
        if not link_config:
            return

        for prop_name, prop_config in link_config.items():
            target_entity_name = prop_config['target_entity']
            target_entity_cls = entities_map[target_entity_name]
            self._crosslink_property(target_entity_cls, prop_name, 
                                     prop_config)

    def get_entity_cls(self):
        if not self.entity_cls:
            raise Exception(
                'Entity "{}" has not been built. Must \
                run the build() method.'.format(self.entity_name)
            )
        return self.entity_cls


    def build_entity(self):
        self._build_entity_cls()
        self._set_field_map()
        self._build_collection_cls()
        self._link_collection_and_entity()
        self._build_properties()
        return (self.entity_cls, self.collection_cls)
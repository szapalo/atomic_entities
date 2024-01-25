import inspect
from typing import Any, List, Mapping
from .. import utils

DataType = Mapping[str, Any]
LstDataType = List[DataType]
LstAny = List[Any]
FieldMap = Mapping[str, utils.Field]

# ------------------------------------------------------------------------------
# ----------------------- Collection -------------------------------------------
# ------------------------------------------------------------------------------

class BaseCollection:
    _entity_cls = None

    def __init__(self, data: LstDataType ) -> None:
        self.data = data
        self._len = len(data)
        self._entities = []
        self._idx = 0
    
    def __len__(self) -> int:
        return self._len

    def __iter__(self):
        return self

    def __getitem__(self, key: int | str):
        match key:
            case int():
                key = key if key >= 0 else self._len - key
                if self._idx <= key :
                    self._fetch_many(key + 1)
                return self._entities[key]
            case str():
                return [d[key] for d in self.data]

    def __next__(self):
        if(self._idx < self._len):
            new_entity = self._entity_cls(self._data[self._idx])
            self._idx += 1
            self._entities.append(new_entity)
            return new_entity
        else:
            raise StopIteration

    @property
    def entities(self):
        match self._idx:
            case 0:
                self._fetch_all()
            case self._len:
                return self._entities
            case _:
                self._fetch_many(self._len)
        return self._entities

    def _fetch_all(self):
        self._entities = [
            self._entity_cls(d) for d in self.data
        ]
        self._idx = self._len
    
    def _fetch_many(self, idx): # TODO : Improve!!
        print("_fetch_many")
        self._entities.extend(
            [self._entity_cls(d) for d in self.data[self._idx: idx]]
        )
        self._idx = idx
    
    def update(self, data: DataType):
        id_field = self._entity_cls._primary_key
        id_name = id_field.name
        expr = id_field.in_([d[id_name] for d in self.data])
        self._entity_cls._DS_API.update(expr, data)
        
        for d in self.data:
            d.update(data)
        

# ------------------------------------------------------------------------------
# ---------------------- BaseEntity --------------------------------------------
# ------------------------------------------------------------------------------

class BaseEntity:
    
    _collection_cls = BaseCollection
    _DS_API = None
    _str: str = None # repr
    _field_map : FieldMap = None 
    _primary_key: utils.Field = None # Field
    _all_keys : List[str] = None #


    def __init__(self, data: DataType, **kwargs):
        self._data = data
        self._id = data[self._primary_key.name]
        self._str = None

    def __str__(self) -> str:
        if not self._str:
            self._str = "{entity_type}(id={id})".format(
                entity_type = self.__class__.__name__,
                id = self._id
            )
        return self._str

    def __repr__(self) -> str:
        return self.__str__()

    def __getitem__(self, item):
        if isinstance(item, utils.Field):
            item = item.name
        if item in self._mapper:
            item = self._mapper[item]
        return self._data[item]

    def __setitem__(self, item, value):
        if isinstance(item, utils.Field):
            item = item.name
        if item in self._mapper:
            item = self._mapper[item]
        self._data[item] = value

    def __call__(self, data: DataType = {}, **kwargs: Any) -> None:
        self._data.update(data)
        self._data.update(kwargs)

    def commit(self) -> None:
        new_data = self._data.copy()
        del new_data[self._primary_key.name]
        self.update(new_data)

    def refresh(self) -> DataType:
        current_data = self._DS_API.findByID(self._id)
        updated_data = {
            k:v for k,v in current_data.items() if v != self._data[k] 
        }
        self._data.update(current_data)
        return updated_data

    # ------------------ Class Methods -----------------------------------------

    @classmethod
    def get_all_keys(cls):
        return cls._all_keys

    @classmethod
    def findByID(cls, id):
        result = cls._DS_API.find_one((cls._primary_key == id,))
        return cls(result) if result else None

    @classmethod
    def findByIDs(cls, id_lst : LstAny) -> BaseCollection:
        return cls._collection_cls(
            cls._DS_API.find((cls._primary_key.in_(id_lst),))
        )

    @classmethod
    def findByKey(cls, key):
        return cls.findByID(key)

    @classmethod
    def findByKeys(cls, key_lst : LstAny):
        return cls.findByIDs(key_lst)

    @classmethod
    def findOne(cls, *args, **kwargs):
        result = cls._DS_API.find_one(args, kwargs)
        return cls(result) if result else None

    @classmethod
    def find(cls, *args, **kwargs):
        return cls._collection_cls(cls._DS_API.find(args, kwargs))

    @classmethod
    def create(cls, data : DataType | LstDataType, **kwargs):
        match data:
            case dict():
                return cls(cls._DS_API.create(dict(data, **kwargs)))
            case list() | tuple() :
                return cls._collection_cls(cls._DS_API.create(
                        [dict(d, **kwargs) for d in data]
                    )
                )
            case _:
                raise Exception(
                    "data argument of create() requires dict or list[dict] type"
                )
    
    # create many
    # @classmethod
    # def create(cls, lst_data: typing.List[dict]=[]):
    #     return cls._collection_cls(
    #         cls._DS_API.create
    #     )
    
    # ------------------ Hybrid Methods ----------------------------------------
    
    def _self_update(self, data):
        self._cls_update((self._primary_key == self._id,), data) 
        self._data.update(data)
        return data

    @classmethod
    def _cls_update(cls, exprs: list, kwargs: dict):
        return cls._DS_API.update(exprs, kwargs)

    @utils.class_or_instance_decorator
    def update(this, *args, **kwargs):
        if args and isinstance(args[-1], dict):
            kwargs = args[-1] | kwargs # we want kwargs to overwrite data
            # kwargs.update(args[-1])
            args = args[:-1]
        if inspect.isclass(this):
            return this._cls_update(args, kwargs)
        else:
            return this._self_update(kwargs)



# ------------------------------------------------------------------------------
# Link Classes
# ------------------------------------------------------------------------------
BaseCollection._entity_cls = BaseEntity
BaseEntity._collection_cls = BaseCollection
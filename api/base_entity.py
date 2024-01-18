import inspect
import typing
from .. import utils


class BaseCollection:
    _entity_cls = None
    _DSC_API = None

    def __init__(self, result : typing.Any) -> None:
        self.result = result
        self._data = []
        self._len = self._DSC_API.len(result)
    
    def __len__(self) -> int:
        return self._len

    def __iter__(self):
        self._idx = -1
        return self

    def __getitem__(self, idx: int):
        l = len(self._data)
        if l < idx :
            self._data.extend(self._DSC_API.fetch_range(self.result, l, idx))
        return self._entity_cls(**self._data[idx])

    def __next__(self):
        self._idx += 1
        if(self._idx < self._len):
            return self._entity_cls(**self._data[self._idx])
        else:
            raise StopIteration

    def data(self):
        self._data.extend([
            self._DSC_API.fetch_remaining(self.result)
        ])
        return self._data

    def entities(self):
        return [self._entity_cls(d) for d in self.data()]

class BaseEntity:
    
    _collection_cls = BaseCollection
    _DS_API = None
    _str = None
    _primary_key = None ## Field
    _all_keys = None

    def __init__(self, data: dict, **kwargs):
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

    def __call__(self, data: typing.Dict[str, typing.Any] = {}, **kwargs: typing.Any) -> typing.Any:
        self._data.update(data)
        self._data.update(kwargs)

    def commit(self):
        self.update(self._data)

    def refresh(self):
        self._DS_API.findOne()
        self._data.update(self._DS_API.findByID(self._id))

    # def delete(self):
    #     self._DS_API.delete(id)

    # //////////////// CLASS METHODS ///////////////////////////////////////////

    @classmethod
    def get_all_keys(cls):
        return cls._all_keys

    @classmethod
    def findByID(cls, id):
        return cls(
            cls._DS_API.find_one((cls._primary_key == id,))
        )

    @classmethod
    def findByIDs(cls, id_lst : typing.List[typing.Any]):
        return cls._collection_cls(
            cls._DS_API.find((cls._primary_key.in_(id_lst),))
        )
    @classmethod
    def findByKey(cls, key):
        return cls.findByID(key)

    @classmethod
    def findByKeys(cls, key_lst : typing.List[typing.Any]):
        return cls.findByIDs(key_lst)

    @classmethod
    def findOne(cls, *args, **kwargs):
        return cls(cls._DS_API.find_one(args, kwargs))

    @classmethod
    def find(cls, *args, **kwargs):
        return cls._collection_cls(cls._DS_API.find(args, kwargs))

    @classmethod
    def createOne(cls, data={}, **kwargs):
        return cls(cls._DS_API.create(data.update(kwargs)))

    # create many
    # @classmethod
    # def create(cls, lst_data: typing.List[dict]=[]):
    #     return cls._collection_cls(
    #         cls._DS_API.create
    #     )
    # //////////////// HYBRID METHODS ///////////////////////////////////////////
    
    def _self_update(self, data):
        self._cls_update((self._primary_key == self._id,), data) 
        self._data.update(data)
        return data
    
    @classmethod
    def _cls_update(cls, exprs: list, kwargs: dict):
        return cls._DS_API.update(*exprs, kwargs)

    @utils.class_or_instance_decorator
    def update(this, *args, **kwargs):
        if inspect.isclass(this):
            return this._cls_update(*args, kwargs)
        else:
            return this._self_update(*args, kwargs)




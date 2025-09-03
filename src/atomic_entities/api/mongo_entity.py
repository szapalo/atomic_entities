
import typing
from bson import ObjectId
from ..utils import RelationalExpr, BooleanOps, Field
import pandas as pd

_ITER_TYPES = (list,tuple)


class MongoAPI:

    _collection = None # collection object
    _base_fields = {
        # 'field_name' : 1 # 1 to inclue, 0 to exclude
        # '_id': 1 # ID is included by default
    }
    # _primary_key = Field("_id")
    _id_fields = ["_id"]

    @classmethod
    def _resolve_expressions(cls, expr: RelationalExpr):
        value = expr.rhs

        if expr.lhs.name in cls._id_fields:
            if isinstance(value, _ITER_TYPES):
                value = [
                    v if isinstance(v, ObjectId) else ObjectId(v)
                    for v in value
                ]
            else:
                value = ObjectId(value)
        match expr.operator:
            case BooleanOps.eq_:
                if isinstance(expr.value, _ITER_TYPES) :
                    op = {"$in" : value}
                else:
                    op = value
            case BooleanOps.in_:
                op = {"$in" : value}
            case BooleanOps.ne_:
                op = {"$ne": value}
            case BooleanOps.le_:
                op = {"$lte": value}
            case BooleanOps.ge_:
                op = {"$gte": value}
            case BooleanOps.lt_:
                op = {"$lt": value}
            case BooleanOps.gt_:
                op = {"$gt": value}
            case _ :
                raise Exception("operator '{}' is not supported".format(expr.operator))
        return op        

    @classmethod
    def _resolve_expressions(cls, exprs: typing.List[RelationalExpr]):
        result = {}
        for expr in exprs:
            if isinstance(expr, dict):
                result.update(expr)
                continue
            result[expr.name] = cls._resolve_expression(expr) 

    # @classmethod
    # def _find_exprs(cls, exprs: list=[], filters: dict = {}):
    #     api_exprs = cls._resolve_expressions(exprs)
    #     api_exprs.update(filters)
    #     return api_exprs

    # @classmethod
    # def _find_query(cls, exprs: list = [], filters: dict = {}, limit=0):


    @classmethod
    def find(cls, exprs: list = [], filters: dict = {}, limit=0, **kwargs):
        api_exprs = cls._resolve_expressions(exprs)
        kwargs.update(filters)
        return list(cls._collection.find(api_exprs, kwargs).limit(limit))

    @classmethod
    def pd_find(cls, *args, **kwargs) -> pd.DataFrame:
        pd.DataFrame(cls.find(*args, **kwargs))

    @classmethod
    def find_one(cls, exprs: list = [], filters: dict = {}):
        api_exprs = cls._resolve_expressions(exprs)
        return cls._collection.find_one(api_exprs, filters)

    @classmethod
    def update(cls, exprs: list = [], data: dict = {}):
        api_exprs = cls._resolve_expressions(exprs)
        return cls._collection.update_many(api_exprs, {"$set": data})
        # handle .update_one ?

    @classmethod
    def insert(cls, data: typing.List[dict]):
        cls._collection.insert_many(data)

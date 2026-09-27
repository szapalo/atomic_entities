import typing
from bson import ObjectId
from .. import utils
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
    def _cast_id(cls, value: str|typing.List[str]):
        if isinstance(value, _ITER_TYPES):
            return [
                v if isinstance(v, ObjectId) else ObjectId(v)
                for v in value
            ]
        else:
            return ObjectId(value)

    @classmethod
    def _resolve_expression(cls, expr: RelationalExpr):
        value = expr.rhs

        if expr.lhs.name in cls._id_fields:
            value = cls._cast_id(value)
        
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
            if isinstance(expr, dict): ### WHAT IS THHIS IF BLOCK??? 
                result.update(expr)
                continue
            result[expr.lhs.name] = cls._resolve_expression(expr) 

    @classmethod
    def _resolve_dict_expressions(cls, mappings: dict):
        return {
            k: {
                '$in' if isinstance(v, _ITER_TYPES) else '$eq': 
                cls._cast_id(v) if k in cls._id_fields else v
            } for k,v in mappings.items()
        }

    # @classmethod
    # def _find_exprs(cls, exprs: list=[], filters: dict = {}):
    #     api_exprs = cls._resolve_expressions(exprs)
    #     api_exprs.update(filters)
    #     return api_exprs

    # @classmethod
    # def _find_query(cls, exprs: list = [], filters: dict = {}, limit=0):

    """
    pipeline = [
        { "$match": { "total": { "$gt": 300 } } },   # like a find()
        {
            "$lookup": {
                from: "customers",
                let: { cid: "$customer_id" },   // inject customer_id from the *orders* document
                pipeline: [
                    { $match: { $expr: { $eq: ["$_id", "$$cid"] } } }
                    { $project: { _id: 0, name: 1 } }                   // select fields
                ],
                as: "customer_info"
            }
        { "$unwind": "$customer_info" }  # flatten the array
        { "limit" : limit}
    ]
    """

    def _resolve_join_table(cls, expr: dict):
        pipeline_match = {}

    @classmethod
    def _resolve_join_expr(cls, exprs: dict):
        for entity_cls, expr in exprs.items():
            tgt_collection = entity_cls._DS_API._collection.name
            


    @classmethod
    def find(cls, exprs: list = [], filters: dict = {}, limit=0, 
             join : dict = {}, **kwargs):
        api_exprs = cls._resolve_expressions(exprs)
        api_exprs.update(cls._resolve_dict_expressions(kwargs))
        

        pipeline = [
            {'$match' : api_exprs},
            {'limit': limit}
        ]
        cls._collection.aggregate()
        # return list(cls._collection.find(api_exprs, kwargs).limit(limit))

    @classmethod
    def find_pd(cls, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(cls.find(*args, **kwargs))

    @classmethod
    def find_one(cls, exprs: list = [], filters: dict = {}, **kwargs):
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

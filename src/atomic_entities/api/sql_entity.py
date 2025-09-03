
import typing
from sqlalchemy import select, update, delete, MappingResult, RowMapping, not_, and_, or_
from .. import utils
from ..utils import RelationalExpr, BooleanOps, LogicalOps, Field, LogicalExpr
import pandas as pd
_ITER_TYPES = (list, tuple)

LogicalOpMap = {
    LogicalOps.and_ : and_,
    LogicalOps.or_  : or_,
    LogicalOps.not_ : not_
}

# class SQLCollectionAPI:
#     @classmethod
#     def to_dict(cls, row: RowMapping):
#         return dict(row)
#     @classmethod
#     def to_list_dict(cls, result, MappingResult):
#         pass
#     @classmethod
#     def len(cls, result: MappingResult):
#         return result.rowcount
#     @classmethod
#     def fetch_range(cls, result: MappingResult, start: int, end: int):
#         return result.fetchmany(end-start)
#     @classmethod
#     def fetch_remaining(cls, result: MappingResult):
#         return result.fetchall()

class SQLAPI:

    # To be defined through SQLEntityFactory    
    # _base_fields = []
    _table_ = None
    _engine_ = None 
    _conn = None
    _base_fields = []
    _select_table = None
    _select_table_one = None
    _select_base_fields = None
    _column_labels_on_join = None
    _update_table = None
    _insert_table = None

    @classmethod
    def _get_column_lst(cls, field_names):
        if(field_names is None):
            return cls._table_.c.values()
        return [
            cls._table_.c[col] for col in field_names
            if col in cls._talbe_.c 
        ]
    
    @classmethod
    def _resolve_expression(cls, expr: RelationalExpr):
        print(f'-------- {expr} ---------- ')
        # curr_column = expr.lhs.entity_cls._DS_API._table_.c[expr.lhs.name] 
        curr_column = cls._table_.c[expr.lhs.name]
        rhs_value = expr.rhs.entity_cls._DS_API._table_.c[expr.rhs.name] \
                    if isinstance(expr.rhs, Field) else expr.rhs
        match expr.operator:
            case BooleanOps.eq_:
                if isinstance(rhs_value, _ITER_TYPES) :
                    op = curr_column.in_(rhs_value)
                else:
                    op = curr_column == rhs_value
            case BooleanOps.in_:
                op = curr_column.in_(rhs_value)
            case BooleanOps.ne_:
                op = curr_column != rhs_value
            case BooleanOps.le_:
                op = curr_column <= rhs_value
            case BooleanOps.ge_:
                op = curr_column >= rhs_value
            case BooleanOps.lt_:
                op = curr_column < rhs_value
            case BooleanOps.gt_:
                op = curr_column > rhs_value
        return op


    @classmethod
    def _resolve_expressions(cls, exprs: typing.List[RelationalExpr]):
        return [cls._resolve_expression(expr) for expr in exprs]

    @classmethod
    def _resolve_join_table(cls, tgt_entity_cls, logical_expr: dict):
        logical_op = list(logical_expr.keys())[0]
        sql_op = LogicalOpMap[logical_op]
        
        relations_lst = logical_expr[logical_op]
        op_args = []
        
        for relation in relations_lst: 
            if isinstance(relation, RelationalExpr):
                op_args.append(tgt_entity_cls._DS_API._resolve_expression(relation))
            elif isinstance(relation, dict): # if logical_expression.resolved()
                op_args.append(cls._resolve_join_table(tgt_entity_cls, relation))
            else:
                raise Exception(f'Expression does not have a valid type: {relation}')

        return sql_op(*op_args)


    @classmethod
    def _resolve_join(cls, stmt, exprs):
        if not exprs:
            return stmt
        result = cls._table_
        
        for entity_cls, expr in exprs.items():
            expr = utils.resolve_join_exprs(expr)
            tgt_table = entity_cls._DS_API._table_

            if isinstance(expr, RelationalExpr):
                resolved_expr = entity_cls._DS_API._resolve_expression(expr)
            elif isinstance(expr, dict): # if LogicalExpression.resolved
                resolved_expr = cls._resolve_join_table(entity_cls, expr)
            else:
                raise Exception(f'Join expression mismatch: {expr}')

            result = result.join(tgt_table, resolved_expr)
            stmt = stmt.add_columns(*entity_cls._DS_API._column_labels_on_join)

        return stmt.select_from(result)

    @classmethod
    def _resolve_filters(cls, filter: dict):
        return [
            (cls._table_.c[k].in_(v)) if isinstance(v, _ITER_TYPES)
            else (cls._table_.c[k] == v)
            for k,v in filter.items()
        ]

    @classmethod
    def _select_expr(cls, exprs: list = [], filters: dict = {}):
        sql_exprs = cls._resolve_expressions(exprs) 
        sql_filters = cls._resolve_filters(filters)
        return cls._select_base_fields.where(*sql_exprs, *sql_filters)

    @classmethod
    def _find_statement(cls, exprs: list = [], filters: dict = {}, limit: int = None, 
             join : dict = {}, **kwargs):
        kwargs.update(filters)
        stmt = cls._select_expr(exprs, kwargs).limit(limit)
        return cls._resolve_join(stmt, join)
    
    @classmethod
    def find_query_str(cls, *args, **kwargs):
        return str(cls._find_statement(*args, **kwargs))

    @classmethod
    def find(cls, *args, **kwargs) -> MappingResult:
        stmt = cls._find_statement(*args, **kwargs)
        result = cls._conn.execute(stmt).mappings().fetchall()
        return list(map(dict, result))

    @classmethod
    def pd_find(cls, *args, **kwargs) -> pd.DataFrame:
        query_str = cls.find_query_str(*args, **kwargs)
        return pd.read_sql(query_str, cls._engine_)
    
    @classmethod
    def find_one(cls, exprs: list = [], filters: dict = {} , **kwargs) -> typing.Dict[str, typing.Any]:
        kwargs.update(filters)
        stmt = cls._select_expr(exprs, kwargs).limit(1)
        result = cls._conn.execute(stmt)
        result = result.mappings().fetchone()
        return dict(result) if result else None

    @classmethod
    def update(cls, exprs: list = [], data: dict = {}, **kwargs):
        kwargs.update(data)
        stmt = cls._update_table.where(
            *cls._resolve_expressions(exprs)
        ).values(**kwargs)
        cls._conn.execute(stmt)
        cls._conn.commit()

    @classmethod
    def insert(cls, data : dict|typing.List[dict] = {}):
        stmt = cls._insert_table.values(data)
        result = cls._conn.execute(stmt)
        cls._conn.commit()
        return result.inserted_primary_key()

    @classmethod
    def delete(cls, exprs: list=[], filters: dict = {}):
        sql_exprs = cls._resolve_expressions(exprs) 
        sql_filters = cls._resolve_filters(filters)
        stmt = cls._delete_table.where(*sql_exprs, *sql_filters)
        cls._conn.execute(stmt)
        cls._conn.commit()


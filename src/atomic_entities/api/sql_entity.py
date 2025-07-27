
import typing
from sqlalchemy import select, update, delete, MappingResult, RowMapping
from ..utils import BinaryExpression, BoolOperators

_ITER_TYPES = (list, tuple)

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
    _select_base_fields_one = None
    _update_table = None
    _insert_table = None

    @classmethod
    def _get_column_lst(self, field_names):
        if(field_names is None):
            return self._table_.c.values()
        return [
            self._table_.c[col] for col in field_names
            if col in self._talbe_.c 
        ]

    @classmethod
    def _resolve_expressions(cls, exprs: typing.List[BinaryExpression]):
        result = []
        for expr in exprs:
            curr_column = cls._table_.c[expr.name]
            match expr.operator:
                case BoolOperators.eq_:
                    if isinstance(expr.value, _ITER_TYPES) :
                        op = curr_column.in_(expr.value)
                    else:
                        op = curr_column == expr.value
                case BoolOperators.in_:
                    op = curr_column.in_(expr.value)
                case BoolOperators.ne_:
                    op = curr_column != expr.value
                case BoolOperators.le_:
                    op = curr_column <= expr.value
                case BoolOperators.ge_:
                    op = curr_column >= expr.value
                case BoolOperators.lt_:
                    op = curr_column < expr.value
                case BoolOperators.gt_:
                    op = curr_column > expr.value
            result.append(op)
        return result

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
    def find(cls, exprs: list = [], filters: dict = {}) -> MappingResult:
        stmt = cls._select_expr(exprs, filters)
        result = cls._conn.execute(stmt).mappings().fetchall()
        return list(map(dict, result))

    @classmethod
    def find_one(cls, exprs: list = [], filters: dict = {} ) -> typing.Dict[str, typing.Any]:
        stmt = cls._select_expr(exprs, filters).limit(1)
        result = cls._conn.execute(stmt)
        result = result.mappings().fetchone()
        return dict(result) if result else None

    @classmethod
    def update(cls, exprs: list = [], data: dict = {}):
        stmt = cls._update_table.where(
            *cls._resolve_expressions(exprs)
        ).values(**data)
        cls._conn.execute(stmt)
        cls._conn.commit()

    @classmethod
    def insert(cls, data : dict|typing.List[dict] = {}):
        stmt = cls._insert_table.values(data)
        cls._conn.execute(stmt)
        cls._conn.commit()
        # TODO : Must return with ID !!!!!!!!!!!!!

    @classmethod
    def delete(cls, exprs: list=[], filters: dict = {}):
        sql_exprs = cls._resolve_expressions(exprs) 
        sql_filters = cls._resolve_filters(filters)
        stmt = cls._delete_table.where(*sql_exprs, *sql_filters)
        cls._conn.execute(stmt)
        cls._conn.commit()

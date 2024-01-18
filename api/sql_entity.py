
import typing
from sqlalchemy import select, update, delete, MappingResult
from ..utils import BoolOperators


class SQLCollectionAPI:
    @classmethod
    def len(cls, result: MappingResult):
        return result.rowcount
    @classmethod
    def fetch_range(cls, result: MappingResult, start: int, end: int):
        return result.fetchmany(end-start)
    @classmethod
    def fetch_remaining(cls, result: MappingResult):
        return result.fetchall()

class SQLAPI:
    
    # _base_fields = []
    _table_ = None
    _engine_ = None 
    _conn = None
    _base_fields = []
    _select_table = None
    _select_base_fields = None
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
    def _resolve_expressions(self, exprs: list):
        result = []
        for expr in exprs:
            match expr.operator:
                case BoolOperators.eq_:
                    if isinstance(expr.value, (list, tuple)) :
                        result.append(self._table_.c[expr.name].in_(expr.value))
                    else:
                        result.append(self._table_.c[expr.name] == expr.value)
                case BoolOperators.in_:
                    result.append(self._table_.c[expr.name].in_(expr.value))
                case BoolOperators.ne_:
                    result.append(self._table_.c[expr.name] != expr.value)
                case BoolOperators.le_:
                    result.append(self._table_.c[expr.name] <= expr.value)
                case BoolOperators.ge_:
                    result.append(self._table_.c[expr.name] >= expr.value)
                case BoolOperators.lt_:
                    result.append(self._table_.c[expr.name] < expr.value)
                case BoolOperators.gt_:
                    result.append(self._table_.c[expr.name] > expr.value)
        return result

    @classmethod
    def _resolve_filters(cls, filter: dict):
        return [
            (cls._table_.c[k].in_(v)) if isinstance(v, (list, tuple))
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
        stmt = cls._select_base_fields.where(exprs, filters)
        return cls._conn.execute(stmt).mappings()

    @classmethod
    def find_one(cls, exprs: list = [], filters: dict = {} ) -> typing.Dict[str, typing.Any]:
        stmt = cls._select_expr(exprs, filters)
        return cls._conn.execute(stmt.limit(1)).mappings().fetchone()

    @classmethod
    def update(cls, exprs: list = [], data: dict = {}):
        stmt = cls._update_table.where(
            *cls._resolve_expressions(exprs)
        ).values(**data)
        cls._conn.execute(stmt)
        cls._conn.commit()

    @classmethod
    def insert(cls, data : dict = {}):
        stmt = cls.__insert_table.values(**data)
        cls._conn.execute(stmt)
        cls._conn.commit()
    
    @classmethod
    def delete(cls, exprs: list=[], filters: dict = {}):
        sql_exprs = cls._resolve_expressions(exprs) 
        sql_filters = cls._resolve_filters(filters)
        cls.__delete_table.where(*sql_exprs, *sql_filters)

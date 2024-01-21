from __future__ import annotations
import typing
from enum import Enum

ExprType: typing.TypeAlias = typing.Any # typing.Union[BinaryExpression"|"CompoundExpression"]

def query_entity_link(self, entity_cls, field_name):
    entity_cls.findByKey(self.key)

def query_entity_links(self, entity_cls, field_name):
    entity_cls.findByKeys(self.key)

class class_or_instance_decorator(classmethod):
    """ This is a descriptor that allows a method to be a @classmethod or 
    instance method.
    """
    def __get__(self, instance, type_):

        if instance is None:
            descr_get = super(class_or_instance_decorator, self).__get__ 
        else:
            descr_get = self.__func__.__get__

        return descr_get(instance, type_)


BoolOperators = Enum(
    'BoolOperators',
    ['in_', 'eq_', 'ne_', 'le_', 'ge_', 'lt_', 'gt_', 'and_', 'or_', 'not_']
)


class Field:
    def __init__(self, name) -> None:
        self.name = name

    def in_(self, __values: list) -> BinaryExpression:
        return BinaryExpression(self.name, __values, BoolOperators.in_)

    def __eq__(self, __value: object) -> BinaryExpression:
        return BinaryExpression(self.name, __value, BoolOperators.eq_)

    def __ne__(self, __value: object) -> BinaryExpression:
        return BinaryExpression(self.name, __value, BoolOperators.ne_)

    def __le__(self, __value: object) -> BinaryExpression:
        return BinaryExpression(self.name, __value, BoolOperators.le_)

    def __ge__(self, __value: object) -> BinaryExpression:
        return BinaryExpression(self.name, __value, BoolOperators.ge_)

    def __lt__(self, __value: object) -> BinaryExpression:
        return BinaryExpression(self.name, __value, BoolOperators.lt_)

    def __gt__(self, __value: object) -> BinaryExpression:
        return BinaryExpression(self.name, __value, BoolOperators.gt_)



class BinaryExpression:
    def __init__(self, name: str, val: typing.Any, op : Enum) -> None:
        self.name = name
        self.value = val
        self.operator = op

    def __and__(self, expr : ExprType ):
        return CompoundExpression(self, expr, BoolOperators.and_)
    
    def __or__(self, expr : ExprType ):
        return CompoundExpression(self, expr, BoolOperators.or_)

    def __invert__(self):
        return CompoundExpression(self, None, BoolOperators.not_)


class CompoundExpression:
    def __init__(self, expr1 : ExprType, expr2 : ExprType, op : Enum) -> None:
        self._expr = (op, expr1, expr2)
    
    def __and__(self, expr : ExprType ) -> CompoundExpression:
        self._expr(BoolOperators.and_, expr, self._expr)
        return self
    
    def __or__(self, expr : ExprType ) -> CompoundExpression:
        self._expr(BoolOperators.or_, expr, self._expr)
        return self

    def __invert__(self, expr : ExprType ) -> CompoundExpression:
        self._expr(BoolOperators.not_, self._expr, None)
        return self

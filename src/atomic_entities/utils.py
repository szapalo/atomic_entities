from __future__ import annotations
import typing

from enum import Enum

ExprType: typing.TypeAlias = typing.Any # typing.Union[RelationalExpr"|"LogicalExpr"]

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



BOOLOPS_ENUM_STR_IDX = [ "in", "==", "!=", "<=", ">=", "<", ">"]
LOCIC_ENUM_STR_IDX = ["&", "|", "~"]

class BooleanOps(Enum):
    in_ = 0
    eq_ = 1
    ne_ = 2
    le_ = 3
    ge_ = 4
    lt_ = 5
    gt_ = 6

    def __str__(self):
        return BOOLOPS_ENUM_STR_IDX[self.value]
    
    def __repr__(self):
        return self.__str__()


class LogicalOps(Enum):
    and_ = 0
    or_ = 1
    not_ = 2

    def __str__(self):
        return LOCIC_ENUM_STR_IDX[self.value]
    
    def __repr__(self):
        return self.__str__()


# BooleanOps = Enum(
#     'BooleanOps',
#     ['in_', 'eq_', 'ne_', 'le_', 'ge_', 'lt_', 'gt_', 'and_', 'or_', 'not_']
# )



class Field:
    def __init__(self, entity_cls, name) -> None:
        self.entity_cls = entity_cls
        self.name = name

    def __repr__(self):
        return f"Field({self.entity_cls}.{self.name})"

    def __contains__(self, __values: list) -> RelationalExpr:
        return RelationalExpr(self, __values, BooleanOps.in_)

    def __eq__(self, __value: object) -> RelationalExpr:
        return RelationalExpr(self, __value, BooleanOps.eq_)

    def __ne__(self, __value: object) -> RelationalExpr:
        return RelationalExpr(self, __value, BooleanOps.ne_)

    def __le__(self, __value: object) -> RelationalExpr:
        return RelationalExpr(self, __value, BooleanOps.le_)

    def __ge__(self, __value: object) -> RelationalExpr:
        return RelationalExpr(self, __value, BooleanOps.ge_)

    def __lt__(self, __value: object) -> RelationalExpr:
        return RelationalExpr(self, __value, BooleanOps.lt_)

    def __gt__(self, __value: object) -> RelationalExpr:
        return RelationalExpr(self, __value, BooleanOps.gt_)



class RelationalExpr:
    def __init__(self, lhs: Field, rhs: typing.Any, op : Enum) -> None:
        self.lhs = lhs
        self.rhs = rhs
        self.operator = op

    def __repr__(self):
        return f"{self.__class__.__name__}( {self.lhs} {self.operator} {self.rhs} )"

    def __and__(self, expr : ExprType ):
        """Overloading '&' operator
        """
        return LogicalExpr(self, expr, LogicalOps.and_)
    
    def __or__(self, expr : ExprType ):
        """Overloading '|' operator
        """
        return LogicalExpr(self, expr, LogicalOps.or_)

    def __invert__(self):
        """Overloading '~' operator
        """
        return LogicalExpr(self, None, LogicalOps.not_)


class LogicalExpr(RelationalExpr):
    def __init__(self, lhs: RelationalExpr|LogicalExpr, 
                 rhs: RelationalExpr|LogicalExpr, op : Enum) -> None:
        super().__init__(lhs, rhs, op)

    def resolve(self, parent_res=None ):
        
        parent_op = list(parent_res.keys())[0] if parent_res else None
        
        if parent_op == self.operator:
            curr_list = parent_res[parent_op]
            result = parent_res
        else:
            curr_list = []
            result = {self.operator: curr_list}
            if parent_op:
                parent_res[parent_op].append(result)

        if isinstance(self.lhs, LogicalExpr):
            self.lhs.resolve(result)            
        else:
            curr_list.append(self.lhs)

        if isinstance(self.rhs, LogicalExpr):
            self.rhs.resolve(result)
        else:
            curr_list.append(self.rhs)

        return result



def extract_join_fields(target_cls, expr_dict: dict):
    result = set()
    for _, expr in expr_dict.items():
        if isinstance(expr, RelationalExpr):
            result.add(
                expr.lhs if expr.lhs.entity_cls == target_cls else expr.rhs
            )
        elif isinstance(expr, dict):
            result.union(extract_join_fields(expr))
    return result



def resolve_join_exprs(exprs, target_entity = None):
    res = exprs
    if type(res) is RelationalExpr:
        return res
    
    if isinstance(exprs, LogicalExpr):
        res = exprs.resolve()
    elif isinstance(exprs, list):
        res = {LogicalOps.and_:  [resolve_join_exprs(expr) for expr in exprs]} 
    elif isinstance(exprs, dict):
        res = {
            LogicalOps.and_: [
                (Field(target_entity, lhs) == rhs)
                for lhs, rhs in exprs.items()]
        }
    else:
        raise Exception(f"Join expression is not a recognizable format : {exprs}")
    return res

    

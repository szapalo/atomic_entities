

p = "./tests/config_sqlite.json"

import json
from pprint import pprint
with open(p) as f:
    config = json.load(f)

from atomic_entities.factories import sql_factory

from atomic_entities import factory
f= factory.Factory(config)

f.build()

A = f.entities_map['A']
B = f.entities_map['B']
A.find_one()._data

# MA = f.entities_map['MonA']
aa = A.find()
bb = B.find()

x = A.insert({'two': 'hello'})

cc = A._DS_API
aa
ex = B.field_map['tb'] == A.field_map['one']

A.find(join={B: ex}).data


ex2 = ex & (B.field_map['id']==2)

A.find(join={B: ex2}).data


# a = A.findOne(one=10)
# a.update(two="lalala")

# aa = A.find()

# from atomic_entities.api.base_entity import EntityCollection as BC, Entity as BE


"""
class A:
    def __init__(self, d):
        self.a = d['a']

from atomic_entities.api import base_entity as be

C = be.EntityCollection
C._entity_cls = A
d = [{'a':i+1000} for i in range(10)]

c = C(d)
"""

def _execute_write(stmt):
    with ds._engine_.begin() as conn:
        return conn.execute(stmt).fetchall()
        # return conn.execute(stmt).scalar()

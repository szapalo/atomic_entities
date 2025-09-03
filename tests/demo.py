
from pprint import pprint
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
# MA = f.entities_map['MonA']

aa = A.find()
bb = B.find()

ex = B.field_map['tb'] == A.field_map['one']

A.find(join={B: ex}).data


ex2 = ex & (B.field_map['id']==2)

A.find(join={B: ex2}).data


# a = A.findOne(one=10)
# a.update(two="lalala")

# aa = A.find()

# from atomic_entities.api.base_entity import BaseCollection as BC, BaseEntity as BE





p = "./atomic_entities/config.json"
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

a = A.findOne(one=10)
a.update(two="lalala")

aa = A.find()

from atomic_entities.api.base_entity import BaseCollection as BC, BaseEntity as BE

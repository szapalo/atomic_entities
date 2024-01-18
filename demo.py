

p = "./AtomicEntities/config.json"
import json

with open(p) as f:
    config = json.load(f)

from AtomicEntities.factories import sql_factory

from AtomicEntities import factory
f= factory.Factory(config)
# f = sql_factory.Factory(conf[0])

# f.build()
# f.build_entities()

f.build()

A = f.entities_map['A']

a = A.findOne(one=10)
a.update(two="lalala")
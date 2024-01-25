# Atomic Entities
Unified API of different data sources.
This package allows for modular ORM-like CRUD operations on datasets the following datasets : 
SQL (SQLite, Postgresql, MySQL, Oracle, MS-SQL, Firebird, Sybase), MongoDB, DynamoDB, Shotgrid. Queried data of a single row/document is encasulated in an  `entity` object whose properties are specified by a config file. 

## Config

The configuration file (json/yaml)


## Example : 
SQLite :

```json
    [
        {
            "ds_type" : "sql",
            "path" : "sqlite://///absolute/path/to/datadase.db"
        }
    ]
```

```python
from atomic_entities as factory

factory.build()

# Get entity class
EntityA = factory.entities_map()

ent_a = EntityA.findOne(id=10)

ent_a = 


```

# 
- Tuple Primary key (more than one field)
- Remove redundant "entity_links" in config, and rely on "target_entity" in "link_properties" 
- Validate existence of fields in config (e.g. properties, base_fields, source_field, target_field)
- Type Annotations for Entities
- Doctring !

1. validate config , for sanity 
1. make primary key optional in config
2. base_keys should include source_field from linked properties 
3. make entities importable
4. dynamic control of base_fields on import
5. add join feature



### Major Change:
Make executions async: 
SQLAlchemy 1.4+ ships an asyncio extension (sqlalchemy.ext.asyncio) with create_async_engine and AsyncSession. It requires an async driver, such as asyncpg for PostgreSQL. And since you decided to stay sync for now, you don't need it yet.

📌 What Changes
	Sync	Async
Engine	create_engine	create_async_engine
Session	Session	AsyncSession
Driver (PostgreSQL)	psycopg2 / psycopg	asyncpg (or psycopg 3 in async mode)
URL	postgresql+psycopg://...	postgresql+asyncpg://...
Execute	session.execute(...)	await session.execute(...)
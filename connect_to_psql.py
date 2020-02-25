from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:okok@localhost:5432/dvdrental')
result = engine.execute("select quote_ident(typname), oid, typdelim, typrelid, typelem, typarray, typtype from pg_type")
result = engine.execute("select table_name, table_schema," 
                        + " is_insertable_into, table_type" +
                        " from information_schema.tables")
for row in result:
    print(row)

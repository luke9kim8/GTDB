import os
import string
import random
import shutil
import subprocess
import numpy as np
import argparse
from datetime import datetime, timedelta

from sqlalchemy import create_engine, SmallInteger, Numeric, Float, Integer, String, BigInteger, Boolean, Date, DateTime, MetaData, ForeignKey, Text, Time
from sqlalchemy.schema import CreateTable
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType

from relmodel import Table, Routine, Column, Op



class CreateSelectQueries(object):
    def __init__(self, uri):
        self.engine = create_engine(uri)
        r = self.engine.execute("SHOW server_version_num")
        for row in r:
           version_num = int(int(row[0]))
        self.version_num = version_num
        self.procedure_is_aggregate = "proisagg" if version_num < 110000 else "prokind = 'a'"
        self.procedure_is_window = "proiswindow" if version_num < 110000 else "prokind = 'w'"
        self.name2type = {}
        self.oid2type = {}
        self.types = []
        self.tables = [] #tables belong in schema.hh, table struct belong in relmodel.hh
        self.operators = [] # needs to belong in schema.py
        self.routines = [] # needs to belong in schema.py
        self.aggregates = [] # needs to belong in schema.py


    def loadTypes(self):
        print("Loading types...")
        rows = self.engine.execute("select quote_ident(typname), oid,"+ 
                                    "typdelim, typrelid, typelem, typarray, " + 
                                    "typtype from pg_type")
        for r in rows:
            pg_type = r
            oid = r[1]
            name = r[0]
            self.name2type[name] = r
            self.oid2type[oid] = r
            self.types.append(r)
        booltype = self.name2type["bool"]
        inttype = self.name2type["int4"]
        internaltype = self.name2type["internal"]
        arraytype = self.name2type["anyarray"]


    def loadTables(self, no_catalog=False):
        print("Loading tables...")
        rows = self.engine.execute("select table_name, table_schema, is_insertable_into, "
                                    + "table_type from information_schema.tables")
        for r in rows:
            schema = str(r[1])
            insertable = str(r[2])
            table_type = str(r[3])
            if (no_catalog and ((schema == "pg_catalog") or (schema == "information_schema"))):
                continue
            table = Table(r[0], schema, True if (insertable == "YES") else False, True if (table_type == "BASE TABLE") else False)
            self.tables.append(table)
        print("done")
    
    def loadColumnsAndConstraints(self):
        self.loadTables(False)
        print("Loading columns and constraints...")
        q = ["select attname, atttypid from pg_attribute join pg_class c on( c.oid = attrelid ) " ,
            "join pg_namespace n on n.oid = relnamespace where not attisdropped and attname not in ",
            "('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid')"]
        q = ''.join(q)
        for table in self.tables:
            q += " and relname = " + "'" + table.name + "'"
            q += " and nspname = " + "'" + table.schema + "'"
            column_results = self.engine.execute(q)
            for result in column_results:
                column = Column(result[0], result[1])
                table.columns().append(column)
        print("done")

    def loadOperators(self):
        print("Loading operators...")
        q = ["select oprname, oprleft,",
		    "oprright, oprresult ",
		    "from pg_catalog.pg_operator ",
                    "where 0 not in (oprresult, oprright, oprleft) "]
        q = ''.join(q)
        rows = self.engine.execute(q)
        for row in rows: 
            op = Op(str(row[0]), int(row[1]), int(row[2]), int(row[3]))
            self.operators.append(op)
        print("done")
    
    def loadRoutines(self):
        print("Loading routines...")
        q = ["select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname ",
	     "from pg_proc ",
	     "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') ",
	     "and proname <> 'pg_event_trigger_table_rewrite_reason' ",
	     "and proname <> 'pg_event_trigger_table_rewrite_oid' ",
	     "and proname !~ '^ri_fkey_' ",
	     "and not (proretset or ", self.procedure_is_aggregate, " or ", self.procedure_is_window, ") "]
        q = ''.join(q)
        result = self.engine.execute(q)
        for row in result:
            rt = Routine(str(row[0]), str(row[1]), int(row[2]), str(row[3]))
            self.routines.append(rt)
        print("done")

    def loadRoutinesParameters(self):
        print("Loading routine parameters...")
        for proc in self.routines:
            q = "select unnest(proargtypes) from pg_proc where oid = " + "'" + proc.specific_name + "'" 
            result = self.engine.execute(q)
            for row in result:

                t = self.oid2type[int(row[0])]
                assert(self.oid2type[int(row[0])])
                proc.argtypes.append(t)
        print("done")
    
    def loadAggregates(self):
        print("Loading aggregates...")
        q = ["select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname ",
	     "from pg_proc ",
	     "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') ",
	     "and proname not in ('pg_event_trigger_table_rewrite_reason') ",
	     "and proname not in ('percentile_cont', 'dense_rank', 'cume_dist', ",
	     "'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc') ",
	     "and proname !~ '^ri_fkey_' ",
	     "and not (proretset or ", self.procedure_is_window, ") ",
	     "and ", self.procedure_is_aggregate]
        q = ''.join(q)
        result = self.engine.execute(q)

        for row in result:
            proc = Routine(str(row[0]), str(row[1]), self.oid2type[row[2]], str(row[3]))
            self.aggregates.append(proc)
        print("done")
    
    def loadAggregateParameters(self):
        print("Loading agregate parameters")
        for proc in self.aggregates:
             q = "select unnest(proargtypes) from pg_proc  where oid = " + proc.specific_name
             result = self.engine.execute(q)
             for row in result:
                 t = self.oid2type[int(row[0])]
                 assert(t)
                 proc.argtypes.append(t)
        print("done")
    def runAll(self):
        self.loadTypes()
        self.loadTables()
        self.loadColumnsAndConstraints()
        self.loadOperators()
        self.loadRoutines()
        self.loadRoutinesParameters()
        self.loadAggregates()
        self.loadAggregateParameters()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--DBURI", required=True,
	help="DB URI to execute select queries")
    args = vars(ap.parse_args())

    csq = CreateSelectQueries(args['DBURI'])
    csq.loadTypes()
    csq.loadTables()
    csq.loadColumnsAndConstraints()
    csq.loadOperators()
    csq.loadRoutines()
    csq.loadRoutinesParameters()
    csq.loadAggregates()
    csq.loadAggregateParameters()

    

if __name__ == "__main__":
    main()
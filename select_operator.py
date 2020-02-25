import os
import string
import random
import shutil
import subprocess
import numpy as np

from datetime import datetime, timedelta

from sqlalchemy import create_engine, Table, Column, SmallInteger, Numeric, Float, Integer, String, BigInteger, Boolean, Date, DateTime, MetaData, ForeignKey, Text, Time
from sqlalchemy.schema import CreateTable
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType
from collections import namedtuple

class CreateSelectQueries(object):
    
    def __init__(self):
        self.engine = create_engine('postgresql://postgres:okok@localhost:5432/dvdrental')
        self.name2type = {}
        self.oid2type = {}
        self.types = []

    def getDataTypes(self):
        rows = self.engine.execute("select quote_ident(typname), oid, typdelim, typrelid, typelem, typarray, typtype from pg_type")
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




def main():
    print("Im main")
    csq = CreateSelectQueries()
    csq.getDataTypes()
    

if __name__ == "__main__":
    main()
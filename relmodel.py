
class Sqltype(object):
    typemap = None
    def __init__(self, name):
        self.name = name
    def get(self, n):
        if n in typemap:
            return typemap[n]
        else:
            typemap[n] = Sqltype(n)
            return typemap[n]
    def consistent(self, rvalue):
        return self == rvalue

class Column(object):
    def __init__(self, name, type=None):
        if (type!=None): 
            self.type = type
        self.name = name
    def __str__(self):
        return ", ".join([str(self.type), self.name])

class Relation(object): 
    def __init__(self, cols=[]):
        self.cols = cols
    def columns(self):
        return self.cols

class Named_relation(Relation):
    def __init__(self, name):
        self.name = name
        super(Named_relation, self).__init__()
    # dont need ident() cuz it just returns name 
    # how should i implement virtual ~named_relation() { }?

class aliased_relation(Named_relation):
    def __init__(self, named, rel):
        super().__init__(named)
        self.rel = rel
    def columns(self):
        print(self.rel.cols)

class Table(Named_relation):
    def __init__(self, name, schema, insertable, base_table):
        self.insertable = insertable
        self.base_table = base_table
        super(Table, self).__init__(name)
        self.schema = schema
    def ident(self):
        return "".join([self.schema, ".", self.name])
    def __str__(self):
        return ", ".join([self.name, self.schema, str(self.insertable), str(self.base_table)])
    

class Routine(object):
    def __init__(self, schema, specific_name, datatype, name):
        assert(datatype)
        self.specific_name = specific_name
        self.schema = schema
        self.restype = datatype
        self.name = name
        self.argtypes = []
    def ident(self):
        if (len(self.schema)):
            return '.'.join([self.schema, self.name])
        else:
            return self.name
class Op(object):
    def __init__(self, name, left, right, result):
        self.name = name
        self.left = left
        self.right = right
        self.result = result
    def __str__(self):
        return ", ".join([self.name, str(self.left), str(self.right), str(self.result)])


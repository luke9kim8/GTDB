class Sqltype(object):
    typemap = {}
    def __init__(self, name):
        self.name = name
    
class Column(object):
    def __init__(self, name, type=None):
        self.name = name
        if type is not None:
            self.type = type
            assert(type)

class Relation(object):
    def __init__(self,name):
        self.name = name
        
    def columns(self):


class Named_relation(Relation):
    name = ""
    def __init__(self, name):
       super().__init__(name)
       self.name = name

class aliased_relation(Named_relation):

        

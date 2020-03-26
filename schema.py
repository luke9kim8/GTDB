from relmodel import Sqltype
from collections import namedtuple

class Schema(object):
    def __init__(self):
        #sqltype variables
        self.booltype = None
        self.inttype = None
        self.internaltype = None
        self.arraytype = None

        #sqltype vectors
        self.types = []
        
        self.tables = []
        self.operators = []
        self.routines = []
        self.aggregates = []

        self.typekey = ()
        self.index = {}
        self.op_iterator = {}
    
    def register_operator(self, o):
        self.operators.append(o)
        self.typekey = (o.left, o.right, o.result)



    
# struct pg_type : sqltype {
#   OID oid_;
#   char typdelim_;
#   OID typrelid_;
#   OID typelem_;
#   OID typarray_;
#   char typtype_;
  
#   pg_type(string name,
# 	  OID oid,
# 	  char typdelim,
# 	  OID typrelid,
# 	  OID typelem,
# 	  OID typarray,
# 	  char typtype)
#     : sqltype(name), oid_(oid), typdelim_(typdelim), typrelid_(typrelid),
#       typelem_(typelem), typarray_(typarray), typtype_(typtype) { }

#   virtual bool consistent(struct sqltype *rvalue);
#   bool consistent_(sqltype *rvalue);
# };
import op
class Pg_type(op):
    def __init__(self, name, oid, typdelim, typrelid, typelem, typarray, typtype):
        self.name = name
        self.oid = oid
        self.typdelim = typdelim
        self.typrelid = typrelid
        self.typelem = typelem
        self.typarray = typarray
        self.typtype = typtype
    

        

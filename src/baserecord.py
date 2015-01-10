#!/usr/bin/python
#-*- coding: utf-8 -*-


from core import DatabaseError, DataManager, Database

__metaclass__ = type

class BaseRelationWrapper(object):
    pass 

class OneToOneRelationWrapper(BaseRelationWrapper):
    def __init__(self, target_cls):
        self._target = target_cls
        self._fields = 

def relation_wrapper_factory(target_cls, base_wrapper_cls):
    O = type(target_cls.__class__.__name__ + "RelationWrapper", 
        (base_wrapper_cls, ), {})

    for name, f_cls target_cls.base_fields.items():
        




class MetaBaseRecord(type):
    """The MetaClass used to accomplish the dynamic generated Record classes"""
    def __init__(cls, name, bases, dct):
        
        # exclude the base class from this behaviour
        if not name == "BaseRecord":                
            cls.table = name.lower()
            
            # move all "*Fields" to self.fields and init atts to None
            cls.base_fields = {}
            for att in cls.__dict__.keys()[:]:
                
                # check for minimal field name length (> 2)
                if len(att) < 2:
                    raise DatabaseError("For __reasons unknown__ field names must have at least 2 chars")

                # identify and setup fields inside this record
                field = getattr(cls, att)
                from fields import AbstractField
                if issubclass(field.__class__, AbstractField):
                    cls.setup_field(att, field)
        
            # this does not behave as expected inside sqlite3 
            # - rowid named col needs AUTOINC, which sux (performance)
            # - so omit this and use built-in rowid, 
            #   which works nicely except for the unintuitive interface (select rowid, * ...)
            #
            # if there is no explicit column named: 'rowid', create one as primary_key!!!
            #if "rowid" not in cls.base_fields:
            #    from fields import IDField
            #    cls.setup_field("rowid", 
            #            IDField(name="rowid", unique=True, primary_key=True))
            
            # init database instance for this class
            cls.database = Database()
            cls.database.contribute(cls)
            
            # populate cls.objects 
            cls.objects = DataManager(cls)

class BaseRecord(object):
    """Every Record Class has to derive from this Class"""
    __metaclass__ = MetaBaseRecord
    
    @classmethod
    def setup_field(cls, name, field):
        """Only for internal use, don't mess with it!"""

        assert not name.startswith("_"), 
            "fieldnames starting with an underscore '_' are not allowed!"
        # reserved keywords, catch...
        assert not name in ["fields", "relations"],
            "'{}' is not allowed as field name".format(name)

        field.name = name
        cls.base_fields[name] = field
        
        if hasattr(cls, name):
            delattr(cls, name)

    def __init__(self, **kw):
        # copy class base_fields to instanc:e
        self.fields = {}
        self.relations = {}
        
        from fields import ManyToOneRelation, OneToManyRelation, \
                OneToOneRelation, ManyToManyRelation, AbstractRelationField 

        for name, field in self.__class__.base_fields.items():
            # all fields are inserted here:
            self.fields[name] = field.clone()

            # fields resembling a relation, additionally here:
            if issubclass(field, AbstractRelationField):
                self.relations[name] = self.fields[name]

        # if there is some keyword-argument, that is not handled by the record, throw exception
        for key in kw:
            if key == "rowid":
                continue

            if not key in self.fields:
                raise DatabaseError("The field/keyword: '{}' was not found in the record".format(key))

        # set this obj as parent for all fields
        for name, field in self.fields.items():
            field.parent = self
        

        # process each defined field
        self.rowid = None
        self.found_primary_key = False 
        for k, v in kw.items():
            field = self.fields.get(k)
    
            # keep field using the primary_key flag and ensure its uniqueness
            if field is not None and field.primary_key:
                if not self.found_primary_key:
                    self.found_primary_key = (k, v)
                else:
                    raise DatabaseError("Found multiple 'primary_key' flagged fields. Inserting: {} Found: {}". \
                            format(k, str(self.found_primary_key)))

            # relation-field -> 1:N
            elif issubclass(field.__class__, OneToManyRelation):
                field.set(v)

            # relation-field -> N:1
            elif issubclass(field.__class__, ManyToOneRelation):
                #rid = v if isinstance(v, int) else v.rowid
                #field.set(field.related_record.objects.get(rowid=rid))
                field.set(v)

            # relation-field -> 1:1
            elif issubclass(field.__class__, OneToOneRelation):
                #rid = v if isinstance(v, int) else v.rowid
                #field.set(field.related_record.objects.get(rowid=rid))
                field.set(v)

            # relation-field -> N:M
            elif issubclass(field.__class__, ManyToManyRelation):
                raise DatabaseError("NOT IMPLEMENTED: ManyToManyRelation")

            # unique-identifier field 
            # (overwritten by user-defined Field, if inside this branch)
            elif k == "rowid":
                if field is not None:
                    field.set(v)
                self.rowid = v

            # regular, all other fields
            else:
                field.set(v)

    # yes save... means WHAT? save all containing items, too???                
    def save(self):
        """Save object in database"""
        ret = self.database.save_obj(self)
        if self.database.lastrowid:
            self.rowid = self.database.lastrowid
        return ret

    def destroy(self):
        """Destroy (delete) object and row in database"""
        return self.database.delete_obj(self)

    def __iter__(self):
        """Iterator that returns (name, value) for this object"""
        for f in ["rowid"] + self.fields.keys():
            yield (f, getattr(self, f))

    def __repr__(self):
        data = [(k,v) if not isinstance(v, list) else \
                    (k, ("<{} rowids=[{}]>". \
                        format(v and v[0].__class__.__name__, ", ".join("{}".format(x.rowid) for x in v))
                    )) for k, v in self]
        return "<{} {}>".format(
                self.__class__.__name__, 
                #" ".join(("{}={}".format(l.encode("utf-8") \
                # if hasattr(l, "encode") else "<None>", r.encode("utf-8") \
                # if hasattr(r, "encode") else "<None>") for l, r in data))
                " ".join(("{}={}".format(l, r) for l, r in data))
            )

    # to access the fields in the record as they were regular attributes
    def __getattr__(self, key):
        if key in self.__class__.base_fields:
            return self.fields[key].get()
        raise AttributeError("The attribute with the name: '{}' does not exist".format(key))
        
    def __setattr__(self, key, val):
        if key in self.base_fields:
            self.fields[key].set(val)
            return 
        object.__setattr__(self, key, val)
   

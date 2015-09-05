#!/usr/bin/python
#-*- coding: utf-8 -*-

from core import DatabaseError, DataManager, SQLiteDatabase, MemoryDatabase

__metaclass__ = type

class MetaBaseRecord(type):
    """The MetaClass used to accomplish the dynamic generated Record classes"""
    def __init__(cls, name, bases, dct):
        
        # exclude the base class from this behaviour
        if object in bases:
            return
        
        cls.table = name.lower()
        
        # move all "*Fields" to self.fields 
        cls.base_fields = {}

        # queue based descent in hierachy to find all necassary fields of
        # arbitrary depth
        workqueue = [(cls, "", att) for att in cls.__dict__.keys()]
        from fields import AbstractField, BaseFieldGroup
        while len(workqueue) > 0:
            curcls, prefix, att = workqueue.pop(0)

            # check for minimal field name length (> 2)
            if len(att) < 2:
                raise DatabaseError("For __reasons unknown__ field " + \
                                    "names must have at least 2 chars")
 
            # identify and setup fields inside this cls
            if not issubclass(curcls.__class__, (AbstractField, BaseFieldGroup)):
                field = getattr(curcls, att)
            else:
                field = curcls
            
            # description TODO ;D
            if issubclass(field.__class__, AbstractField):
                cls.setup_field(prefix + att, field)

            elif issubclass(field.__class__, BaseFieldGroup):
                cls.setup_field(prefix + att, field)

                for sub_field_key, sub_field in field.grp_fields.items():
                    if issubclass(sub_field.__class__, BaseFieldGroup):
                        workqueue.append((sub_field, prefix + att + "__", 
                            sub_field_key))

                    elif issubclass(sub_field.__class__, AbstractField):
                        cls.setup_field(prefix + att + "__" + sub_field_key, 
                                sub_field)
        
        # init database instance for this class
        cls.database = SQLiteDatabase()
        cls.database.contribute(cls)
        
        # populate cls.objects 
        cls.objects = DataManager(cls)
            
class BaseRecord(object):
    """Every record class has to derive from this class"""
    __metaclass__ = MetaBaseRecord
    
    @classmethod
    def setup_field(cls, name, field):
        """(internal) handles name assignment for new fields, and moves it"""

        # no starting underscore "_" in fieldname
        assert not name.startswith("_"), \
            "fieldnames starting with an underscore '_' are not allowed!"
        
        # reserved keywords, catch...
        assert not name in ["fields", "table", "dirty"], \
            "'{}' is not allowed as field name".format(name)

        field.name = name
        cls.base_fields[name] = field.clone()
        
        if hasattr(cls, name):
            delattr(cls, name)

    def __init__(self, **kw):
        # copy class base_fields to instance
        self.fields = {}
        
        from fields import ManyToOneRelation, OneToOneRelation, \
                ManyToManyRelation, BaseFieldGroup

            
        for name, field in self.__class__.base_fields.items():
            self.fields[name] = field.clone()
            self.fields[name].name = name
        
        # check for a non-existing passwd keyword
        for key in kw:
            if key == "rowid":
                continue

            if not key in self.fields:
                raise DatabaseError("The field/keyword: '{}' was not found " + \
                                    "in the record".format(key))

        # set this obj as parent for all fields
        for name, field in self.fields.items():
            field.parent = self
        
        # 'dirty'-flag ... 'True' -> needs to be saved
        self.dirty = True
        
        # keyword assignment
        add_kw = {}
        for name in kw.keys():
            field = self.fields.get(name)
            if isinstance(field, BaseFieldGroup):
                for k in field.key2field.keys():
                    add_kw[name + "__" + k] = getattr(kw.get(name), k)
        kw.update(add_kw)

        # process each defined field
        self.rowid = None
        self.found_primary_key = False 

        for k, v in kw.items():
            field = self.fields.get(k)
            # keep field using the primary_key flag and ensure its uniqueness
            if field is not None \
                    and hasattr(field, "primary_key") \
                    and field.primary_key:

                if not self.found_primary_key:
                    self.found_primary_key = (k, v)
                else:
                    raise DatabaseError("Found multiple 'primary_key' " + \
                            "flagged fields. Inserting: {} Found: {}". \
                            format(k, str(self.found_primary_key)))

            # handle index col
            elif k == "rowid":
                if field is not None:
                    field.set(v)
                self.rowid = v

            # regular, all other fields
            else:
                field.set(v)
        
        for k, v in self.fields.items():
            if isinstance(v, BaseFieldGroup):
                v.update_cls()
             
    # yes save... 
    def save(self):
        """Save object in database"""
        from fields import AbstractRelationField
        # check, if any related fields must be saved before
        #for name, f in self.fields.items():
        #    if isinstance(f, AbstractRelationField) and self.get(a
        #        if f.dirty:
        #            f.save()
                
        # save me!!!
        ret = self.database.save_obj(self)
        if self.database.lastrowid:
            self.rowid = self.database.lastrowid
        self.dirty = False
        return ret

    def destroy(self):
        """Destroy (delete) object and row in database"""
        return self.database.delete_obj(self)

    def __iter__(self):
        """Iterator that returns (name, value) for this object"""
        for f in ["rowid"] + self.fields.keys():
            fobj = getattr(self, f)
            
            if isinstance(fobj, BaseRecord):
                yield (f, "REF")
            else:
                yield (f, fobj)
            
    def __repr__(self):
        """Should show the INSTANCE attributes, and omit the class/field ones"""
        field_maxlen = 6
        data = [(k,v) if not isinstance(v, list) else \
                    (k, ("<{} rowids=[{}]>". \
                        format(v and v[0].__class__.__name__, 
                               ", ".join("{}".format(x.rowid) for x in v))
                    )) for k, v in self]
        return "<{} {}>".format(
                self.__class__.__name__, 
                " ".join(("{}=<{}>".format(k, v) for k, v in data))
            )

    # access field-contents
    def get(self, key):
        if key in self.__class__.base_fields:
            return self.fields[key].get()
        raise AttributeError("The attr with the name: '{}' does not exist". \
                format(key))
    
    def set(self, key, val):
        if key in self.__class__.base_fields:
            self.dirty = True
            self.fields[key].set(val)
            return 
        object.__setattr__(self, key, val)
   
    # to access the fields in the record as they were regular attributes
    def __getattr__(self, key):
        return self.get(key)
    
    # and set them as if they were attributes...
    def __setattr__(self, key, val):
        return self.set(key, val)

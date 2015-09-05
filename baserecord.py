#!/usr/bin/python
#-*- coding: utf-8 -*-


from core import DatabaseError, DataManager, SQLiteDatabase, MemoryDatabase

__metaclass__ = type

#class BaseRelationWrapper(object):
#    def __init__(self, target_cls, obj, ex): 
#        # target class relate to
#        self._target = target_cls
#        # my own object instance (row/record)
#        self._parent = obj
#        # expression used for matching/join
#        self.expr = ex 
#
#    def _get_db_interface(self):
#        raise NotImplementedError()
#    
# Nearly (transparent) wrapper for a single object 
# one2one: table1.id <-> table2.id
#class OneToOneRelationWrapper(BaseRelationWrapper):
#    # to access the fields in the record as they were regular attributes
#    def __getattr__(self, key):
#        if key in self._target.base_fields:
#            return self._get_db_interface()(_=self.expr)
#        raise AttributeError("The attribute with the name: '{}' does not exist".format(key))
#        
#    def __setattr__(self, key, val):
#        if key in self._target.base_fields:
#            raise NotImplementedError("found field, but setting not supported, yet...TODO")
#        object.__setattr__(self, key, val)
#
#    def _get_db_interface(self):
#        return self._target.objects.get
#
## handles like a list - sounds _CRAZY_
## one2many: table1.id <-> [table2.ref_id, ...]
#class OneToManyRelationWrapper(list, BaseRelationWrapper):
#    def __init__(self, target_cls, obj, ex):
#        list.__init__()
#        BaseRelationWrapper.__init__(target_cls, obj, ex)
#
#    def _get_db_interface(self):
#        return self._target.objects.filter
#
#
#def relation_wrapper_factory(target_cls, base_wrapper_cls):
#    return type(target_cls.__class__.__name__ + "RelationWrapper", 
#        (base_wrapper_cls, ), {})


class MetaBaseRecord(type):
    """The MetaClass used to accomplish the dynamic generated Record classes"""
    def __init__(cls, name, bases, dct):
        
        # exclude the base class from this behaviour
        #if not name == "BaseRecord":                
        if object in bases:
            return
        
        cls.table = name.lower()
        
        # move all "*Fields" to self.fields 
        cls.base_fields = {}
        for att in cls.__dict__.keys()[:]:
            
            # check for minimal field name length (> 2)
            if len(att) < 2:
                raise DatabaseError("For __reasons unknown__ field " + \
                                    "names must have at least 2 chars")
 
            # identify and setup fields inside this record
            field = getattr(cls, att)
            from fields import AbstractField, BaseFieldGroup
            if issubclass(field.__class__, AbstractField):
                cls.setup_field(att, field)

            elif issubclass(field.__class__, BaseFieldGroup):
                cls.setup_field(att, field, field_group=True)

                for sub_field_key, sub_field in field._fields.items():
                    cls.setup_field(att + "__" + sub_field_key, sub_field, 
                            sub_field=True)
        #print cls.base_fields
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
        cls.database = SQLiteDatabase()
        cls.database.contribute(cls)
        
        # populate cls.objects 
        cls.objects = DataManager(cls)
            
class BaseRecord(object):
    """Every Record Class has to derive from this Class"""
    __metaclass__ = MetaBaseRecord
    
    @classmethod
    def setup_field(cls, name, field, field_group=False, sub_field=False):
        """Only for internal use, don't mess with it!"""

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
            # all fields are inserted here---just for reference
            self.fields[name] = field #.clone()

        # if there is some keyword-argument, that is not handled by the record, 
        # throw exception
        for key in kw:
            if key == "rowid":
                continue

            if not key in self.fields:
                raise DatabaseError("The field/keyword: '{}' was not found " + \
                                    "in the record".format(key))

        # set this obj as parent for all fields
        for name, field in self.fields.items():
            field.parent = self
        
        # is this record-obj 'dirty' (has been changed since last save())
        self.dirty = True
        
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

            #relation-field -> 1:N
            #elif issubclass(field.__class__, OneToManyRelation):
            #    #field.set(v)
            #    #field.setup_relation(
            #    print v
            #    raise TypeError("Cannot set 1:N relation...")
            #
            # relation-field -> N:1
            #elif issubclass(field.__class__, ManyToOneRelation):
                #rid = v if isinstance(v, int) else v.rowid
                #field.set(field.related_record.objects.get(rowid=rid))
            #    field.set(v)

            # relation-field -> 1:1
            #elif issubclass(field.__class__, OneToOneRelation):
                #rid = v if isinstance(v, int) else v.rowid
                #field.set(field.related_record.objects.get(rowid=rid))
            #    field.set(v)

            # relation-field -> N:M
            #elif issubclass(field.__class__, ManyToManyRelation):
            #    raise DatabaseError("NOT IMPLEMENTED: ManyToManyRelation")

            # unique-identifier field 
            # (overwritten by user-defined Field, if inside this branch)
            elif k == "rowid":
                if field is not None:
                    field.set(v)
                self.rowid = v

            # regular, all other fields
            else:
                field.set(v)

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
        raise AttributeError("The attr with the name: '{}' does not exist".format(key))
    
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
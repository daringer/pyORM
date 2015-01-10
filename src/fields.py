#!/usr/bin/python
#-*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod, abstractproperty
import time

from core import DatabaseError
from baserecord import BaseRecord 
from field_expression import FieldExpression

#from config import Config

__metaclass__ = type


UNIQUE_ROW_ID_NAME = "rowid"


# fancier use of properties
#Property = lambda func: property(**func()) 

class AbstractField(object):
    """Every Field class derives from this class"""
    __metaclass__ = ABCMeta
    
    # various field flags (globally accepted keywords)
    general_keywords = ["name", "size", "default", "primary_key", "required", "unique", "auto_inc"]
    default = None        # default value
    name = None           # name of field/column
    size = None           # storing size in bytes (precision)
    required = False      # value required to be set on insert/update
    primary_key = False   # (the only!) primary_key inside table (row identity)
    unique = False        # each row has a unique value (for this column)
    auto_inc = False      # automatically increments column on each insert

    # keep Field-specific applicable keywords
    accepted_keywords = []
   
    ###
    # ACTUALLY NEED PROPERTIES TO REALIZE READ-ONLY STUFF, BUT NO GOOD SOLUTION FOUND YET!
    ###

    def __init__(self, **kw):
        # check, if kw contains solely legal keywords
        wrong = [k for k in kw if not k in (self.accepted_keywords + self.general_keywords)]
        if len(wrong) > 0:
            raise DatabaseError("Keyword(s): {} not supported by this Field: {}". \
                    format(", ".join(wrong), self.__class__.__name__))
        
        # setting FieldObjects as member names
        for k, v in kw.items():
            setattr(self, k, v) 

        # apply the various general flags/values
        for k in self.general_keywords:
            val = kw.get(k) if k in kw else getattr(self.__class__, k)
            setattr(self, k, val)

        # keeps the explicit value of this field (and it's object, if applicable)
        self._value = self.default if not "default" in kw else kw["default"]
        self._value_obj = None

        # parent record class
        self.parent = None
        # keep the passed keyword-dict from instanciation
        self.passed_kw = kw

    # if an attribute is in "accepted_keywords", but not set return "None"
    def __getattr__(self, key):
        if key in self.accepted_keywords + self.general_keywords:
            return None
        raise AttributeError(key)

    def clone(self):
        """Mainly internal use - returnes a clone (copy) of the AbstractField"""
        return self.__class__(**self.passed_kw)

    def get_create(self, prefix=None, suffix=None):
        """Get universal attributes needed for create column query"""
        out = prefix or ""

        # make this column unique across this table
        if self.unique:
            out += " UNIQUE"
        
        # make this column the primary_key column
        if self.primary_key:
            out += " PRIMARY KEY ASC"

        # change sql-stmt, if column is required
        if self.required:
            out += " NOT NULL"
        
        # always set default value for each column
        default_out = " DEFAULT NULL"
        if self.default is not None:
            default_out = " DEFAULT {}".format(self.get_escaped(default=True))
        out += default_out

        # set column to automatically increment itss value on insert
        if self.auto_inc:
            out += " AUTOINCREMENT"

        if suffix is not None:
            out += suffix

        return out

    def set(self, v):
        """Set field value to 'v'"""
        self._value = v
        self._value_obj = None

    def get(self):
        """Get field (raw) value"""
        return self._value

    def get_escaped(self, default=False):
        """Get field value escaped/quoted - for sql update/insert"""
        return self._value if not default else self.default

    def pre_save(self, action="insert", obj=None):
        """This is called directly before saving (action: 'update' or 'insert') the object"""
        return True
        
    def post_save(self, action="insert", obj=None):
        """This is called directly after the object was saved (action: 'update' or 'insert')"""
        return True 

    # rich comparision methods, 
    # all return a FieldExpression for lazy evaluation
    def __lt__(self, other):
        return FieldExpression(self, other, operator.lt)

    def __le__(self, other):
        return FieldExpression(self, other, operator.le)

    def __eq__(self, other):
        return FieldExpression(self, other, operator.eq)

    def __ne__(self, other):
        return FieldExpression(self, other, operator.ne)

    def __gt__(self, other):
        return FieldExpression(self, other, operator.gt)

    def __ge__(self, other):
        return FieldExpression(self, other, operator.ge)

    def __contains__(self, other):
        return FieldExpression(self, other, operator.contains)

    def __len__(self):
        return FieldExpression(self, None, len)

    def __and__(self, other):
        return FieldExpression(self, other, operator.and_)

    def __xor__(self, other):
        return FieldExpression(self, other, operator.xor)

    def __or__(self, other):
        return FieldExpression(self, other, operator.or_)
    
    def __invert__(self):
        return FieldExpression(self, None, operator.inv) 

    def __add__(self, other):
        return FieldExpression(self, other, operator.add)

    def __sub__(self, other):
        return FieldExpression(self, other, operator.sub)

    def __mul__(self, other):
        return FieldExpression(self, other, operator.mul)

    def __div__(self, other):
        return FieldExpression(self, other, operator.div)

            

class IntegerField(AbstractField):
    """Store a single integer value. 
    The backend should provide at least 32bit signed
    """
    accepted_keywords = ["foreign_key"]
    default = 0

    # TODO: add different sizes
    def get_create(self, prefix=None, suffix=None):
        out = "{} INT".format(self.name)
        out = super(IntegerField, self).get_create(prefix=out)
        if prefix:
            out = prefix + out
        if suffix:
            out = out + suffix
        return out

class IDField(IntegerField):
    """Store the unique row identification."""
    name = UNIQUE_ROW_ID_NAME
    primary_key = True
    unique = True

class BooleanField(IntegerField):
    """Store a single boolean values."""
    default = False

    def __init__(self, **kw):
        super(BooleanField, self).__init__(**kw)
        assert self.default in [True, False, 0, 1]

    def get_create(self, prefix=None, suffix=None):
        return (prefix or "") \
               + super(BooleanField, self).get_create() \
               + (suffix or "")

    def set(self, val):
        assert val in [True, False, 0, 1]
        self._value = val in [True, 1]

    def get_escaped(self, default=False):
        v = self._value if not default else self.default
        return 1 if v in [True, 1] else 0

class DateTimeField(IntegerField):
    """Store a single date and time values."""
    accepted_keywords = ["auto_now", "auto_now_add"]
  
    def get_create(self, prefix=None, suffix=None):
        return (prefix or "") \
               + super(DateTimeField, self).get_create() \
               + (suffix or "")

    def pre_save(self, action="insert", obj=None):
        if (action == "insert" and (self.auto_now_add or self.auto_now)) or \
           (action == "update" and self.auto_now):
            self.set(int(time.time()))
        return True

    def get_fancy_time(self):
        return FancyTime(self._value).get()

    def get_fancy_datetime(self):
        return FancyDateTime(self._value).get()

    def get_fancy_date(self):
        return FancyDate(self._value).get()
      
class FloatField(AbstractField):
    """Store a floating point number.
    The backend should provide at least 32bit
    """
    default = 0.0    
    
    def get_create(self, prefix=None, suffix=None):
        out = "{} FLOAT".format(self.name)
        return (prefix or "") \
               + super(FloatField, self).get_create(prefix=out) \
               + (suffix or "")

class BlobField(AbstractField):
    """Store a raw binary block of any size."""
    def get_create(self, prefix=None, suffix=None):
        out = "{} BLOB".format(self.name)
        return (prefix or "") \
                + super(BlobField, self).get_create(prefix=out) \
                + (suffix or "")

class StringField(AbstractField):
    """Store a string, i.e., some "useful" string - includes stripping..."""
    accepted_keywords = ["foreign_key"]
    default = ""
    
    def get_create(self, prefix=None, suffix=None):
        out = "{} {}".format(self.name, 
                "TEXT" if self.size > 255 else "VARCHAR", 
                "({})".format(self.size) if self.size else ""
            )
        return (prefix or "") \
               + super(StringField, self).get_create(prefix=out) \
               + (suffix or "")

    def get_escaped(self, default=False):
        v = self._value if not default else self.default
        return "'{}'".format(v)

    def pre_save(self, action="insert", obj=None):
        if self._value is None:
            return not self.required

        self.set(self._value.strip())
        return True

class OptionField(StringField):
    """Store one of the provided options as string."""
    accepted_keywords = ["options"]
    
    def __init__(self, options, **kw):
        # options must be a regular list of strings, iterable is ok
        assert hasattr(options, "__iter__") and len(options) > 1
        self.options = options

        # keep provided options
        kw.update({"options": options})
    
        # set size, if not manually done...
        self.size = max(map(len, options))

        # cross-check own size with provided and so on
        if kw.get("size") is not None and self.size <= kw.get("size"):
            raise DatabaseError("provided size ({}<{}) is less than biggest provided option: {}". \
                    format(kw.get("size"), self.size, ", ".join(options)))
    
        # the default value defaults to first item of options, if no 'default' is set
        if "default" in kw:
            assert kw["default"] in options
            self.default = kw["default"]
        else:
            self.default = options[0]
        kw["default"] = self.default
        
        # call parent's ctor
        super(OptionField, self).__init__(**kw)

    def set(self, val):
        assert val in self.options
        self._value = val

class AbstractRelationField(AbstractField):
    accepted_keywords = ["related_record"]
    
    def __init__(self, related_record, name=None, related_field=None, **kw):
        assert issubclass(related_record, BaseRecord)
        
        self.name = name or self.name

        kw.update({"related_record" : related_record})
        
        #if related_field is None:
        #    related_field = "re_" + (name or self.__class__.__name__);
        #return self._value

        super(AbstractRelationField, self).__init__(**kw)
        
        self.related_record = related_record
        self.related_field = related_field
        self.name = name

    def get(self):
        raise NotImplementedError()

    def set(self, val):
        raise NotImplementedError()

    def get_create(self, prefix=None, suffix=None):
        out = "{} {}".format(self.name, "INT")
        return (prefix or "") \
               + super(AbstractRelationField, self).get_create(prefix=out) \
               + (suffix or "")


class OneToManyRelation(AbstractRelationField):
    def get(self):
        q = {self.related_field: self.parent.rowid}
        return self.related_record.objects.get(**q)

    def set(self, val):
        self._value = val

class ManyToOneRelation(AbstractRelationField):
    def get(self):
        #q = {self._field: self.parent.rowid}
        self,objec

    def set(self, val):
        self._value = val
            
class ManyToManyRelation(AbstractRelationField):
    def get(self):
        return self._value

    def set(self):
        self._value = val

class OneToOneRelation(AbstractRelationField):
    def get(self):
        return self._value

    def set(self, val):
        self._value = val
 
##### the object-based interface - to be done, but explicit!!!
##### - means no in-transparent "magic" to 
#####   return objects one time and IDs another time
#    def get_obj(self):
#        """Get associated field object"""
#        if self._value_obj is not None:
#            return self._value_obj
#        return self.related_record.objects.get(rowid=self._value)
#
#    def set_obj(self, v):
#        """Set associated field object"""
#        self._value.rowid
#        self._value_obj = v

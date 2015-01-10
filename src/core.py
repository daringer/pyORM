#!/usr/bin/python
#-*- coding: utf-8 -*-

import sqlite3 as sqlite
from threading import Lock

__metaclass__ = type

class DatabaseError(Exception):
    pass

class Database(object):
    """Low-Level Object-Based Database Interface"""

    # locking mechanism!
    lock = Lock()

    # mmmh, system wide list of contributing records?! 
    contributed_records = []

    # save the number of done queries
    query_counter = 0

    # set to 'True' to get all plaintext sql-queries in 'stdout'
    debug = False

    # central db-connection keeping:
    db_file = None 
    db_con = None 
    
    def init(self, db_file, force=False):
        """init database connection"""

        if Database.db_con is None or force is True:
            Database.db_file = db_file
    
    def close(self, force=False):
        if Database.db_con is not None or force is True:
            if Database.db_con is not None:
                Database.db_con.close()
            Database.db_file = None
            Database.db_con = None

    def reset(self):
        if Database.db_con is not None:
            self.close()
        Database.query_counter = 0
        Database.contributed_records = []

    def contribute(self, cls):
        """Every Record has to "register" here"""
        
        # handle relation fields
        from fields import ManyToOneRelation, OneToManyRelation, \
                OneToOneRelation, ManyToManyRelation

        for field in cls.base_fields.values():
            #####
            ##### backref= should be discussed (TODO)
            fieldname = "re_" + cls.table
            if isinstance(field, OneToManyRelation):
                field.related_record.setup_field(cls.table, ManyToOneRelation(cls, name=fieldname))
            elif isinstance(field, ManyToOneRelation):
                field.related_record.setup_field(cls.table, OneToManyRelation(cls))
            elif isinstance(field, OneToOneRelation):
                field.related_record.setup_field(cls.table, OneToOneRelation(cls, name=fieldname))
            elif isinstance(field, ManyToManyRelation):
                field.related_record.setup_field(cls.table, ManyToManyRelation(cls, name=fieldname))
            #####
            #####

        self.contributed_records += [cls]

    def create_tables(self):
        """Check for all 'cls', if we need to create the needed table"""
        
        for rec in self.contributed_records:             
            # there is no 'show tables' sql-statement in sqlite so query sqlite_master
            q = "SELECT * FROM sqlite_master WHERE type='table' AND tbl_name='{}'".format(rec.table)
            res = self.query(q)
            
            # table was found - skip creation...
            if len(res) == 1:
                continue

            ## here we haven't found the table
            # first check for field-number > 0
            if len(rec.base_fields) == 0:
                raise DatabaseError("Could not create table: {}, there are no fields defined for it!".format(rec.table))

            # actually build creation of table-query
            q = "CREATE TABLE {} ({})". \
                    format(rec.table, ", ".join(x.get_create() \
                        for x in rec.base_fields.values() if x.name)
                    )
            self.query(q)
       
    def query(self, q, args=()):
        """Actually performing a query. Replace variables in 'q' with "?" and
        pass the variables in the second argument 'args' as tupel
        """
        
        if self.debug:
            print "SQL-Query: |- {} -| values: {}".format(q, args)

        self.query_counter += 1
               
        with self.lock:
            if Database.db_con is None:
                Database.db_con = sqlite.connect(Database.db_file)
            
            # to return a dict for each row
            Database.db_con.row_factory = sqlite.Row
            
            # to auto-commit
            Database.db_con.isolation_level = None
            
            ### text-encoding 
            #self.db_con.text_factory = sqlite.OptimizedUnicode
            Database.db_con.text_factory = unicode
            
            self.cursor = Database.db_con.cursor()
            self.cursor.execute(q, args)
            self.lastrowid = self.cursor.lastrowid

            out = self.cursor.fetchall() if q.lower().startswith("select") else True            

        return out
           
    def save_obj(self, obj):
        """Either insert the object if "rowid" is found in table,
        or update if rowid if found in the table
        """
        
        # does it already exist ? DOIN' NOTHIN' HERE!!!!!??!
        q = "SELECT rowid FROM {} WHERE rowid=?".format(obj.table)
        
        # determine action (act) - either "update" or "insert"
        act = "update" if obj.rowid and self.query(q, (obj.rowid,)) else "insert"
        
        # prepare all fields to be saved using Field::pre_save()
        for attr in obj.fields:
            if not obj.fields[attr].pre_save(action=act, obj=obj):
                raise DatabaseError("Field::pre_save() for field '{}' with value '{}' failed". \
                        format(attr, getattr(obj, attr)))
     
        # collect data (omit empty-fields, pseudo-fields)
        attr_vals = [{"col": k, "val": v.get()} for k, v in obj.fields.items() \
                if v.get() is not None]

        # replace BaseRecord descendants with their .rowid 
        from baserecord import BaseRecord
        for i in xrange(len(attr_vals)):
            if issubclass(attr_vals[i]["val"].__class__, BaseRecord):
                attr_vals[i]["val"] = attr_vals[i]["val"].rowid
        
        # construct the sql query 
        # --- UPDATE
        if act == "update":
            fields = ",".join((x["col"] + "=?") for x in attr_vals)
            #fields = ", ".join("{}=?".format(x["col"] for x in attr_vals))
            q = "UPDATE {} SET {} WHERE rowid={}".format(obj.table, fields, obj.rowid)
        # --- INSERT
        elif act == "insert":
            fields = ",".join(x["col"] for x in attr_vals)
            vals = ",".join(["?"] * len(attr_vals))
            q = "INSERT INTO {} ({}) VALUES ({})".format(obj.table, fields, vals)
        
        # executing constructed sql-query
        ret = self.query(q, [x["val"] for x in attr_vals])    
        
        # postprocess fields using Field::post_save()
        for attr in obj.fields:
            if not obj.fields[attr].post_save(action=act, obj=obj):
                raise DatabaseError("Field::post_save() for field '{}' with value '{}' failed". \
                        format(attr, getattr(obj, attr)))
        return ret
            
    def delete_obj(self, obj):
        """Delete given object from the database"""

        q = "DELETE FROM {} WHERE rowid=?".format(obj.table)
        return self.query(q, (obj.rowid,))
        
    def filter(self, cls, operator="=", limit=None, order_by=None, **kw):
        """Return instances of 'cls' according to given values in 'kw' from
        the Database 
        """
       
        # check if the passed keywords exist as field
        all_fields = cls.base_fields.keys() + ["rowid"]
        if any(not k in all_fields for k in kw):
            raise DatabaseError(".filter got a non-field keyword (one of: {}), instead of one of: '{}'". \
                    format(", ".join(kw.keys()), ", ".join(all_fields)))
                     
        # postprocess the query keywords
        from fields import ManyToOneRelation
        from baserecord import BaseRecord
        for k, v in kw.items():
            if k in cls.base_fields and isinstance(cls.base_fields[k], ManyToOneRelation):
                kw[k] = v.rowid if v else None
        
        # use 'kw'-dict as WHERE CLAUSE
        if kw:
            # uhu ugly-magic, actually just replacing the operator with "IS NULL" if the kw value is None
            where = " AND ".join("{}{}". \
                    format(k, (operator + "?" if v is not None else " IS NULL")) \
                    for k, v in kw.items()
            )
            
            q = "SELECT rowid, * FROM {} WHERE {}".format(cls.table, where)

        # or simply select all
        else:
            q = "SELECT rowid, * FROM {}".format(cls.table)    
        
        # --- ORDER BY
        if order_by:
            if any(not x.strip("+-") in all_fields for x in order_by):
                raise DatabaseError("'order by' contains non field keys: {}, availible are only: {}". \
                        format(", ".join(order_by), ", ".join(all_fields)) )
        
            q += " ORDER BY {}".format(
                    ", ".join("{}{}".format(x[0], " DESC" if x[1].startswith("-") else "") \
                            for x in order_by))
        
        # --- LIMIT
        if limit:
            q += " LIMIT %s,%s" % limit
        
        vals = [x for x in kw.values() if x is not None] if kw else []      
        return [cls(**kw) for kw in self.query(q, vals)]
    
    def exclude(self, cls, **kw):
        """Return instances which DON'T match the 
        keyword -> value combination from kw"""

        return self.filter(cls, operator="<>", **kw)
    
class DataManager(object):
    """Object managing class placed as AnyRecord.objects"""
    def __init__(self, rec, pre_filter={}, order_by=None, limit=None):
        self.record = rec
        self.pre_filter = pre_filter

        # order_by must be a tuple of fieldnames with a leading "+" or "-", no operator implies "+"
        self.order_by = order_by

        # limit must be a tuple of 2 elements
        self.limit = limit

    def __repr__(self):
        return "<%s DataManager>" % self.record.__name__

    def __getitem__(self, key):
        """Behave like a list of objects, 'key' is a non-database/result-only related up counting index"""

        # implement slicing! TODO FIXME
        if not isinstance(key, (int, long)):
            raise TypeError("'{}' (type: {}) is not a valid index". \
                    format(str(key), key.__class__.__name__))

        # slice -> .start .stop .step .... isinstance(key, slice)

        return self.record.database.filter(self.record, limit=(key,1))[0]
    
    def all(self):
        """Return all Record objects from the database"""
        return self.record.database.filter(self.record)

    def filter(self, **kw):
        """This is the access method to all rows aka objects from the database.
        use like this: 
        MyRecord.objects.filter(some_field="bar", other_field="foo")
        """

        kw.update(self.pre_filter)
        return self.record.database.filter(self.record, **kw)

    def one(self, **kw):
        """Returns exactly one item, if appropriate. 
        Does not throw Exception on error, simply return 'None'
        """

        try:
            return self.get(**kw)
        except DatabaseError as e:
            return None

    def get(self, **kw):
        """Returns exactly one object if found or None. 
        raises an DatabaseError, if more than one is found
        """

        ret = self.all() if len(kw) == 0 else self.filter(**kw)

        if len(ret) == 1:
            return ret[0]
        elif len(ret) > 1:
            raise DatabaseError("Got more than one row from: {}".format(kw))
        
        # not found - return None
        return None

    def exists(self, **kw):
        """Checks for existance for a row with the given {key:value} pairs"""
        return self.one(**kw) is not None

    def exclude(self, **kw):
        """Exclude the objects with match the keyword -> value combination passed"""
        return self.record.database.exclude(self.record, **kw)

    def create_or_get(self, **kw):
        """'kw' requires at least one table-unique column/keyword.
        returns the found object, if it is found, or
        returns a fresh created (and saved) object with given data
        """
        
        # if no unique keyword is given, raise exception
        if not any(self.record.base_fields[k].unique for k in kw \
                if k in self.record.base_fields):
            raise DatabaseError("The kw-dict does not contain a unique key")
        
        # get unique keywords and use them to self.objects.get() an object  
        unique_kw = dict((k, v) for k,v in kw.items() \
                if k in self.record.base_fields and \
                   (self.record.base_fields[k].unique or \
                    self.record.base_fields[k].primary_key)
        )
        
        # create or get data-object for unique_fields
        u = self.record.objects.get(**unique_kw)
        if not u:
            u = self.record(**kw)
            u.save()

        return u




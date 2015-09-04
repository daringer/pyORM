
import os, sys
import time 
import unittest 

sys.path.append("..")

from baserecord import BaseRecord
from fields import StringField, IntegerField, DateTimeField, \
        OneToManyRelation, FloatField, OptionField, ManyToOneRelation, \
        ManyToManyRelation
from core import Database



class FieldTestSuite(unittest.TestCase):

    def setUp(self):
        db_name = ":memory:"
        d = self.db = Database()
        d.init(db_name)
    
    def tearDown(self):
        self.db.reset()
        self.db.close()

    def test_int_field(self):
        class MyModel(BaseRecord):
            num = IntegerField()
        
        self.db.create_tables()
        data = 123
        data2 = 999

        m = MyModel(num=data)
        self.assertTrue(m.num == data)
        self.assertTrue(m.rowid is None)

        m.save()
        self.assertTrue(m.rowid is not None)
        self.assertTrue(m.num == data)

        m.num = data2 
        m.save()
        self.assertTrue(m.rowid is not None)
        self.assertTrue(m.num == data2)
        self.assertFalse(m.num == data)

    def test_string_field(self):
        class MyModel(BaseRecord):
            word = StringField(size=40)

        self.db.create_tables()
        data = "something"
        data2 = "and something other"

        m = MyModel(word=data)
        self.assertTrue(m.word == data)
        self.assertTrue(m.rowid is None)
        
        m.save()
        self.assertTrue(m.rowid is not None)
        self.assertTrue(m.word == data)

        m.word = data2
        m.save()
        self.assertTrue(m.rowid is not None)
        self.assertTrue(m.word == data2)
        self.assertFalse(m.word == data)

 
if __name__ == '__main__':
    unittest.main()

import os, sys
import time 
import unittest 

import operator as ops

sys.path.append("..")

from baserecord import BaseRecord
from fields import StringField, IntegerField, DateTimeField, \
        OneToManyRelation, FloatField, OptionField, ManyToOneRelation, \
        ManyToManyRelation
from core import Database

from field_expression import FieldExpression 

class FieldExpressionTestSuite(unittest.TestCase):

    def setUp(self):
        pass
    
    def tearDown(self):
        pass


    def test_simple_exp_add(self):
        x, y = 1, 2
        for o in [ops.add, ops.sub, ops.and_, ops.or_, ops.eq, ops.lt, ops.le]:
            f1 = FieldExpression(x, y, o)
            self.assertTrue(f1.eval() == o(x, y), "failed: {}, ref: {}". \
                    format(o.__name__, o(x, y)))

    def test_partly_evaluation(self):
        x, y = 123, "varname"
        f1 = FieldExpression(x, y, ops.add)
        f2 = f1.eval()
        self.assertTrue(isinstance(f1.eval(), FieldExpression))
        self.assertTrue(f1.to_string() == "123 + varname")
        f1.context["varname"] = 313
        self.assertTrue(isinstance(f1.eval(), int))
        self.assertTrue(f1.eval() == 436)

    def test_multi_level(self):
        x, y, z = 423, 324, 321
        f1 = FieldExpression(x, y, ops.add)
        f2 = FieldExpression(y, z, ops.add)
        f3 = FieldExpression(f1, f2, ops.sub)
        self.assertTrue( f3.eval() == ops.sub(ops.add(x, y), ops.add(y, z)) )
        self.assertTrue( f3.to_string() == "(423 + 324) - (324 + 321)" )

    
    def test_direct_operator_use(self):
        x, y, z = 423, 324, 321
        res = "(((423 + 324) + (324 + 321)) - (423 + 324)) + (324 + 321)"
        f1 = FieldExpression(x, y, ops.add)
        f2 = FieldExpression(y, z, ops.add)
        f3 = ( ( f1 + f2 ) - f1) + f2
        self.assertTrue( f3.eval() == 1290 )
        self.assertTrue( f3.to_string() == res )

if __name__ == '__main__':
    unittest.main()

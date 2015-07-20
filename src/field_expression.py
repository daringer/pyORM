import operator

class FieldExpressionError(Exception):
    pass

class FieldExpression(object): 
    operator_map = {
    
        operator.eq: "{} == {}",
        operator.le: "{} <= {}",
        operator.lt: "{} <  {}",
        operator.ne: "{} <> {}",
        operator.gt: "{} >  {}",
        operator.ge: "{} >= {}",
        operator.contains: "{} in {}",
        len: "count({})",
        operator.and_: "{} && {}",
        operator.or_:  "{} || {}",
        operator.xor: "{} ^ {}",
        operator.inv: "~{}",
        operator.add: "{} + {}",
        operator.sub: "{} - {}",
        operator.mul: "{} * {}",
        operator.div: "{} / {}",
    }

    operator_one_arg = set((len, operator.inv))

    def __init__(self, arg1, arg2=None, op=None, obj1=None, obj2=None, context=None):
        self.context = context or {}
        self.arg1 = arg1 
        self.arg2 = arg2 

        # not in operator_map
        if op is not None and op not in self.operator_map:
            raise FieldExpressionError(
                "'{}' not an legal operator".format(op))
        
        # 2 args, but 'op' is just for a single
        if self.arg2 is not None and op in self.operator_one_arg:
            raise FieldExpressionError(
                "'{}' operation takes only ONE argument, found two...".\
                        format(op.__name__))

        # 1 arg, but 'op' is for two
        if self.arg2 is None and op not in self.operator_one_arg:
            raise FieldExpressionError(
                "'{}' operation takes exactly TWO arguments, found only one...".\
                        format(op.__name__))
        
        # passed two args without an operation
        if op is None and self.arg1 is not None and self.arg2 is not None:
            raise FieldExpressionError(
                "'arg1' and 'arg2' are set, an 'op'eration is mandatory!")

        self.op = op 

        # only used, if arg(s) are (Base)Field(s)
        self.obj1 = obj1
        self.obj2 = obj2

    def _prepare_arg(self, arg, obj, recurse=None, apply_ctx=True):
        from fields import AbstractField 

        # arg == FieldExpression (recurse)
        if isinstance(arg, FieldExpression):
            return recurse(arg)
        
        ### context AND field specilization is too much, 
        ### include field into context TODO FIXME
        # arg == AbstractField
        elif isinstance(arg, AbstractField):
            if apply_ctx:
                if not isinstance(obj, arg):
                    raise FieldExpressionError() 
                return getattr(obj, arg.name)
            else:
                return obj.name + "." + arg.name
            
        # arg in self.context
        elif arg in self.context:
            return self.context[arg] if apply_ctx else arg
        
        # arg == others
        else:
            return arg

    def eval(self, obj1=None, obj2=None, context=None):
        # passed directly get precendence, if applicable
        self.obj1 = obj1 or self.obj1
        self.obj2 = obj2 or self.obj2 
        self.context.update(context or {})

        visitor = lambda x: x.eval()
        # prepare arg1
        arg1 = self._prepare_arg(self.arg1, self.obj1, visitor)
        # prepare arg2 
        if self.arg2 is not None:
            arg2 = self._prepare_arg(self.arg2, self.obj2, visitor)

        # try applying operation and return!
        try:
            if self.arg2 is None:
                # 'arg1' may be alone and without 'op'
                if self.op is None:
                    return arg1 
                return self.op(arg1)
            else:
                return self.op(arg1, arg2)
        except TypeError as e:
            # trying operation on incompatible types -> unresolved symbol
            # returning partly-evaluated FieldExpression-clone instead!
            return FieldExpression(arg1, arg2, self.op, self.obj1, self.obj2, self.context)
                                   
    def to_string(self, obj1=None, obj2=None, context=None, apply_ctx=False):
        # passed directly get precendence, if applicable
        self.obj1 = obj1 or self.obj1
        self.obj2 = obj2 or self.obj2 
        self.context.update(context or {})

        visitor = lambda x: "(" + x.to_string() + ")"
        
        # prepare arg1
        arg1 = self._prepare_arg(self.arg1, self.obj1, visitor, apply_ctx=apply_ctx)
        # prepare arg2
        if self.arg2 is not None:
            arg2 = self._prepare_arg(self.arg2, self.obj2, visitor, apply_ctx=apply_ctx)

        # get string-template, format and return
        tmpl = self.operator_map.get(self.op)
        if self.arg2 is None:
            # 'arg1' may be alone and without 'op'
            if self.op is None:
                return arg1 
            return tmpl.format(arg1)
        else:
            return tmpl.format(arg1, arg2)
     
    # FieldExpressions may contain FieldExpressions 
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




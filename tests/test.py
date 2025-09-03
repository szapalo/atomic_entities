


from atomic_entities.utils import RelationalExpr, Field as F, LogicalExpr as LE

f1 = F('A','a')
f2 = F('A','b')
f3 = F('A','c')
f4 = F('A','d')
f5 = F('A','e')
f6 = F('A','f')
e1 = f1 == f2
e2 = f3 == f4
e3 = f5 == f6
x = e1 & e2 & e3
d = x.resolve()



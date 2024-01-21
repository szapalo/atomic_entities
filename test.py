
x = 10000
it = 100000

arr = [[(i,i)] for i in range(x)]

for _ in range(it):
    r = [dict(i) for i in arr]
    # r = list(map(dict,arr))

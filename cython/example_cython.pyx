# def test(x):
#     y = 0
#     for i in range(x):
#         y += i
#     return y

def test(int x):
    cdef int y = 0
    cdef int i
    for i in range(x):
        y += i
    return y

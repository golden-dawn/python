import timeit

cy = timeit.timeit('''example_cython.test(5)''',setup='import example_cython',number=100)
py = timeit.timeit('''example_original.test(5)''',setup='import example_original', number=100)

print(cy, py)
print('Cython is {}x faster'.format(py/cy))

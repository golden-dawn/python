from distutils.core import setup
from Cython.Build import cythonize

setup(name='JL', ext_modules=cythonize("stxjl.pyx"))

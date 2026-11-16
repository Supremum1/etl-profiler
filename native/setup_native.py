from Cython.Build import cythonize
from setuptools import Extension, setup
from pathlib import Path

source = Path(__file__).with_name("_writer_core.pyx")

setup(
    name="etl-native-writer",
    ext_modules=cythonize(
        [Extension("_writer_core", [str(source)], extra_compile_args=["-O3"])],
        compiler_directives={"language_level": "3"},
    ),
)

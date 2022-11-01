import re
import setuptools
from distutils.core import setup
from pathlib import Path

name = "de-assignment"
version = "1.0"
py_version = "3.9"

# recursively get a list of packages under src
package_dirs = {x.parts[-1]: str(x) for x in Path().glob("src/*/*_*") if "." not in str(x)}
packages = []
for package_dir in Path("src").iterdir():
    packages += setuptools.find_packages(where=str(package_dir), exclude=["*pycache*", "*tests*"])

with open("environment.yml", "r") as file:
    data = file.read()
    required_libraries = re.findall(r"^\s*-\s*(\w.*\w)\s*#\s*bundle", data, re.M)

setup(
    name=name,
    version=version,
    package_dir=package_dirs,
    packages=packages,
    install_requires=required_libraries,
    include_package_data=True,
    author="Atem Tacktic",
)

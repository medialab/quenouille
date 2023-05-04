from setuptools import setup, find_packages

with open("./README.md", "r") as f:
    long_description = f.read()

setup(
    name="quenouille",
    version="1.8.0",
    description="A library of multithreaded iterator workflows.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/medialab/quenouille",
    license="MIT",
    author="Guillaume Plique",
    author_email="kropotkinepiotr@gmail.com",
    keywords="url",
    python_requires=">=3.5",
    packages=find_packages(exclude=["test"]),
    package_data={"docs": ["README.md"]},
    extras_require={
        ":python_version<'3.8'": ["typing_extensions"],
    },
    zip_safe=True,
)

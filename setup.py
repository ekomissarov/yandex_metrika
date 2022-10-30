import setuptools
# https://packaging.python.org/tutorials/packaging-projects/

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysea-yandex-metrika", # Replace with your own username
    version="0.0.1",
    author="Eugene Komissarov",
    author_email="Yandex Metrika base",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ekomissarov/yandex_metrika.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=[

    ]
)
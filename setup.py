import setuptools
   
setuptools.setup(
    name="ParallelPyMetaMap",
    version="0.1.0",
    author="Antoine Lain, Jamie Campbell, Ian Simpson",
    author_email="Antoine.Lain@ed.ac.uk, Ian.Simpson@ed.ac.uk",
    description="This code is to run PyMetaMap in parallel.",
    packages=setuptools.find_packages(),
    classifiers=[
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
'pandas',
'requests',
'numpy'
],
    python_requires='>=3.6'
)

import setuptools
   
setuptools.setup(
    name="ParallelPyMetaMap",
    version="0.1.0",
    author="Jamie Campbell, Ian Simpson, Antoine Lain",
    author_email="Jamie.campbell@igmm.ed.ac.uk, Ian.Simpson@ed.ac.uk, Antoine.Lain@ed.ac.uk",
    description="This projects is to run PyMetaMap in parallel to get biomedical annotations from published literature.",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
'pandas',
'numpy',
'pathlib',
'datetime',
'pathlib'],
    python_requires='>=3.6'
)
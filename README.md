# ParallelPyMetaMap
This code is to run PyMetaMap in parallel

## Requirements

In order to run the code, you will need a few things:

1) Metamap.

2) To git clone and install the repository.

3) A Cadmus DataFrame output is prefered or a dataframe where the texts you want to process are contained within a column.

## Installation
ParallelPyMetaMap has a number of dependencies on other Python packages, it is recommended to install in an isolated environment

`git clone https://github.com/biomedicalinformaticsgroup/ParallelPyMetaMap.git`

`pip install ./ParallelPyMetaMap`

You can now remove the file if you want using:

`rm -rf ParallelPyMetaMap`

## Get started
ParallelPyMetaMap is utilizing the number of cores available in your machine to create parallel instances of MetaMap server to run multiple rows at the same time. To do so, the library is using the multiprocessing library. 

The aim of the library is to be able to annotate biomedical publication with the UMLS. For that, we are using a modified version on PyMetamap. One of the modifications allows the data sources restrictions which mean you can tailor the annotation to your interest. 

It was created to be flexible and to automatically distribute  the data to the number of cores you are providing to the script. While running the processes, it is saving the output regularly in case something happen. Once all the processes are done the script will merge all the results in one dataframe called 'master_annotated_working_df2.p'.

Like Cadmus, dynamic generation is important. You can update your annotated dataframe as your Cadmus output grows without re-annotating previously annotated publications.

```python
from ParallelPyMetaMap import ppmm

ppmm(NUMBER_OF_CORES_TO_USE, 
                    PATH_TO_METAMAP)
```

On top of these two mandatory parameters we have:
- path_to_file: You can provide the directory to the file you want to annotate.
- file: You can input the DataFrame directly into the script instead of providing the path to the file.
- restrict_to_sources: Metamap contains multiple [data sources](https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html), you can provide a list of data sources you would like to restrict Metamap to.
- verbose: verbose is an extra parameter for your confort. When set to False, you will not see the live processing of MetaMap to be able to focus on the warning and the current status of the processing. When changing from False to True, you will add the MetaMap processing information on top of our information

## Important
 [MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html) comes with a license. Please make sure you are following the terms of the licence when using MetaMap and its result. 

## Extra resources
[MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html)

## FAQ

Q: One or multiple of my processes are not sending results, what should I do?

A: You should first use the command 'top' (or similar command), in the result you should find a row called METAMAP(version).BINARY, if one of the process exceeded 3 hours, most likely the process will have difficulty to process this current row. You can choose to take the PID number of the process and then run on your terminal 'kill PID number'. When you do that the library will just ignore the current line and continue to the next one.

Q: How to determine the number of cores? 

A: There is no perfect answer to that question. Be mindful of the number of cores available on your machine. If you request more than present on your machine the code will not start. Please check first the number of cores on your machine and second the number of cores available before starting the process. If you are not careful with the number you selected, the machine might freeze and you will need to restart it. 

Q: Using the parameter restrict_to_sources, I receive a warning saying the data source is not present in my vocabulary, why that ? 

A: We do not know or check which version of MetaMap you are using, please read the documentation from [MetaMap Dataset](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/additional-tools/DataSetDownload.html) to see if the data source you requested is available for your installed version. 



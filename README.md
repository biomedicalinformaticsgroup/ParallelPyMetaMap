# ParallelPyMetaMap
This code is to run PyMetaMap in parallel

## Requirements

In order to run the code, you will need a few things:

1) Metamap

2) To git clone the project to the directory you want to save your result.

3) A Cadmus DataFrame output or a dataframe where the text you want to process has the column's name 'content_text'

## Installation
ParallelPyMetaMap has a number of dependencies on other Python packages, it is recommended to install in an isolated environment

`git clone https://github.com/biomedicalinformaticsgroup/ParallelPyMetaMap.git`

`pip install ./ParallelPyMetaMap`

## Get started
ParallelPyMetaMap is utilizing the number of core available in your machine to create parallel instances of MetaMap server to run multiple rows at the same time. To do so, the library is using the multiprocessing library. 

The aim of the library is to be able to annotate biomedical publication with the UMLS. For that, we are using a modified version on PyMetamap. One of the modification allows the data sources restrictions which mean you can tailor the annotation to your interest. 

It was created to be flexible and to automatically distribute  the data to the number of core you are providing to the script. While running the processes are saving the output regularly in case something happen. Once all the processes are done the script will merge all the results in one dataframe called 'master_annotated_working_df2.p'.

Like Cadmus, dynamic generation is important. You can update your annotated dataframe as your Cadmus output grows without re-annotating previously annotated publications.

```python
from ParallelPyMetaMap import parralelism_metamap

parralelism_metamap(NUMBER_OF_CORE_TO_USE, 
                    PATH_TO_METAMAP)
```

On top of these two mandatory parameters we have:
- path_to_file: You can provide the directory to the file you want to annotate.
- file: You can input the DataFrame directly instead of the path to the file.
- restrict_to_sources: Metamap contains multiple data sources, you can provide a list of data sources you would like to restrict Metamap to.
- verbose: verbose is an extra parameter for your confort. When set to False, you will not see the live processing of MetaMap to be able to focus on the warning and the current status of the processing. When changing from False to True, you will add the MetaMap processing information on top of our information

## Important
 [MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html) comes with a license. Please make sure you are following the terms of the licence when using MetaMap and its result. 

## Extra ressources
[MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html)

## FAQ

Q: One or multiple of my processes are not sending results, what should I do?

A: You should first use the command 'top' (or similar command), in the result you should find a row called METAMAP(version).BINAR, if the process exceded 3 hours, most likely the processes will have difficulty to process this current row. You can choose to take the PID number of the process and then run on your terminal 'kill PID number'. When you do that the library will just ignore the current line and continue to the next process.

Q: How should to determine the number of core? 

A: There is no perfect answer to that question. Be mindful of the number of core available on your machine. If you request more than present on your machine the code will not start. Please check first the number of core on your machine and second the number of core available before starting the process. If you are not carefull with the number you selected, the machine might froze and you will need to restart it. 

Q: Using the parameter restrict_to_sources, I receive a warning saying the data source is not present in my vocaulary, why that ? 

A: We do not know or check which version of MetaMap you are using, please read the documention from [MetaMap Dataset](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/additional-tools/DataSetDownload.html) to see if the data source you requested is available for your installed version. 



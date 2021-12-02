# ParallelPyMetaMap
This code is to run MetaMap in parallel using Python.

## Requirements

In order to run the code, you will need a few things:

1) Metamap installed locally.

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

The aim of the library is to be able to annotate biomedical publication with the UMLS. For that, we are using a modified version on PyMetamap. We offer a wider range of options available in MetaMap, extra information is saved during the process and we collate all the information in one dataframe. 

It was developed to be flexible and to automatically distribute the data to the number of cores you are providing to the script. While running the processes, it is saving the output regularly in case something happen. Once all the processes are done the script will merge all the results in one dataframe. In case of faillure, it has a restart option that will start from the last save.

Like Cadmus, dynamic generation is important. You can update your annotated dataframe as your input grows without re-annotating previously annotated publications.

```python
from ParallelPyMetaMap import ppmm

ppmm(NUMBER_OF_CORE(S)_TO_USE, 
    PATH_TO_METAMAP,
    file or path_to_file,
    column_name, 
    unique_id,
    machine_output or (fielded_mmi_output and extension_format)
    )
```

Mandatory parameters:
- number_of_core(s)_to_use: this is a mandatory parameter with no default value. Here, you need to insert the number of core(s) you want the function to use. 
- path_to_MetaMap: this is again a mandatory parameter with no default value. You need to insert the path to your MetaMap instance in the bin directory of MetaMap.
- column_name = 'content_text': this parameter as 'content_text' has default value, which is the column that contains the text from cadmus. You need to input the name of the column where the text you want to annotate is stored. 
- unique_id = 'pmid': the default value is 'pmid', again this value has been choose if your input is from cadmus. You need here to provide the name of the column that can act as a key. It has to be unique, without space and no escape characters. 
- extension_format = None: this parameter depends if you are running the code using fielded_mmi_output or machine_output. The default value is None, the None value will only works if you are running the code using machine_output. If you are using fielded_mmi_output, you need to decide if you want your annotated files to be structured like in the terminal then extension_format = 'terminal' or if you prefere your files to be Python like dictionaries then extension_format = 'dict'. 
- path_to_file = None or file = None: both of these parameters have None has default value, you are required to only change one of the two. 'path_to_file' takes a string to the destination of the dataframe, in a pickle object, you want to annotate. 'file' you can directly input your Pandas DataFrame.

Optional parameters:
- extension = 'txt': 'extension' has 'txt'has default value. During the process we save the annotation produced by MetaMap in a file, here you can set the extension of these files. 
- restart = False: in case your code faces some kind of faillure while running, you can switch the default value from False to True. In that case, the function will start by looking at the temporary file(s) to restart from the previous save. 
- verbose = False: MetaMap can show you the sentences has it processes them. The default value is set to False. When set to False, the function only shows a summary of where the function is at. If you want to see the sentences being processed live, change the default to True.

You can find bellow the list of all the MetaMap options implemented in ParallelPyMetaMap with their associated default values:
- composite_phrase=4
- fielded_mmi_output=False
- machine_output=False
- filename=None
- file_format='sldi'
- allow_acronym_variants=False
- word_sense_disambiguation=False
- allow_large_n=False
- strict_model=False
- relaxed_model=False
- allow_overmatches=False
- allow_concept_gaps=False
- term_processing=False
- no_derivational_variants=False
- derivational_variants=False
- ignore_word_order=False
- unique_acronym_variants=False
- prefer_multiple_concepts=False
- ignore_stop_phrases=False
- compute_all_mappings=False
- prune=False
- mm_data_version=False
- mm_data_year=False
- exclude_sources=[]
- restrict_to_sources=[]
- restrict_to_sts=[]
- exclude_sts=[]
- no_nums=[]

You can find out more about these options on the National Library of Medecine website: [Documentation: Using MetaMap & options](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/UsingMetaMap.html), you will usually find the information about the options in the 'Usage Notes'.

## Load the result

The output from ParallelPyMetaMap is a Pickle object. In order to open the result use the following two lines of code.

```python
import pickle
retrieved_df = pickle.load(open('./output_ParallelPyMetaMap_{column_name}_{mmi (for fielded_mmi_output) or mo (for machine_output)}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'rb'))

## Fielded MMI Output Structure

In order to produce a Fielded MMI Output you need to use the following structure of code:
```python
from ParallelPyMetaMap import ppmm

ppmm(NUMBER_OF_CORE(S)_TO_USE, 
    PATH_TO_METAMAP,
    file or path_to_file,
    column_name, 
    unique_id,
    fielded_mmi_output = True,
    extension_format = ('dict' or 'terminal')
    )
```

The main output is a pandas dataframe saved as a pickle object.
This is stored in the directory ```"./output_ParallelPyMetaMap_{column_name}_mmi/annotated_df/annotated_{column_name}_{unique_id}_df2.p"```. The dataframe columns are:

- cui <class 'str'>
  - UMLS Concept Unique Identifier (CUI) – The CUI for the identified UMLS concept.
- umls_preferred_name <class 'str'>
  - The preferred name for the UMLS concept identified in the text. 
- semantic_type <class 'list'>
  - Comma separated list of Semantic Type abbreviations for the identified UMLS concept. 
- full_semantic_type_name <class 'list'>
  - Comma separated list of Semantic Type full name for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt)
- semantic_group_name <class 'list'>
  - Comma separated list of Semantic Group full name for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt)
- occurrence <class 'numpy.int64'>
  - Number of times this CUI has been found in the text in total.
- annotation <class 'dict'>
  - You can find more about the annotation from the MetaMap documentation (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/MMI_Output.pdf). 
- unique_id <class 'str'>
  - You provide this value in the input to identify which text we are mentioning.

Other Outputs:
- ```"./output_ParallelPyMetaMap_{column_name}_mo/annotated_df/annotated_{column_name}_{unique_id}_df.p"``` this Pandas DataFrame is used for updates, to store the result while the function is running before merging the result at the end to the main DataFrame.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/extra_resources"``` contains a {unique_id}_to_avoid.txt that keeps the {unique_id} of failed attempts to annotate a text due to either insuficent memory or that they exceed the timeout. It also has 2 Pandas DataFrames that have the Semantic Type full name and the Semantic Group full name.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/temporary_df"```, this directory is used by each core allocated to the code to save the processed rows in case of faillure. 
- ```"./output_ParallelPyMetaMap_{column_name}_mo/{extension_format}_files"``` is composed of files with all the annotations from MetaMap. Files are saved using {unique_id}.{extension_format}.

## Machine Output Structure

In order to produce a Machine Output you need to use the following structure of code:
```python
from ParallelPyMetaMap import ppmm

ppmm(NUMBER_OF_CORE(S)_TO_USE, 
    PATH_TO_METAMAP,
    file or path_to_file,
    column_name, 
    unique_id,
    machine_output = True
    )
```

The main output is a pandas dataframe saved as a pickle object.
This is stored in the directory ```"./output_ParallelPyMetaMap_{column_name}_mo/annotated_df/annotated_{column_name}_{unique_id}_df2.p"```. The dataframe columns are:

- cui <class 'str'>
  - UMLS Concept Unique Identifier (CUI) – The CUI for the identified UMLS concept.
- prefered_name <class 'str'>
  - The preferred name for the UMLS concept identified in the text. 
- semantic_type <class 'list'>
  - Comma separated list of Semantic Type abbreviations for the identified UMLS concept. 
- full_semantic_type_name <class 'list'>
  - Comma separated list of Semantic Type full name for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt)
- semantic_group_name <class 'list'>
  - Comma separated list of Semantic Group full name for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt)
- occurrence <class 'numpy.int64'>
  - Number of times this CUI has been found in the text in total.
- negation <class 'numpy.int64'>
  - Number of times this CUI has been found in the text in a negative/absent context.
- trigger <class 'list'>
  - List of string(s) showing what triggered MetaMap to identify this UMLS concept.
- sab <class 'list'>
  - SAB (Abbreviated Source Name) (https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html)
- pos_info <class 'list'>
  - List of positional information doubles showing StartPos, /, and Length of each trigger identified.
- score <class 'list'>
  - The score has a maximum value of 1000.00. The higher the score, the greater the relevance of the UMLS concept according to MetaMap. When the trigger was consider has negative/absent the score is negative in that case the higest value is -1000.00.
- unique_id <class 'str'>
  - You provide this value in the input to identify which text we are mentioning.

Other Outputs:
- ```"./output_ParallelPyMetaMap_{column_name}_mo/annotated_df/annotated_{column_name}_{unique_id}_df.p"``` this Pandas DataFrame is used for updates, to store the result while the function is running before merging the result at the end to the main DataFrame.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/extra_resources"``` contains a {unique_id}_to_avoid.txt that keeps the {unique_id} of failed attempts to annotate a text due to either insuficent memory or that they exceed the timeout. It also has 2 Pandas DataFrames that have the Semantic Type full name and the Semantic Group full name.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/temporary_df"```, this directory is used by each core allocated to the code to save the processed rows in case of faillure. 
- ```"./output_ParallelPyMetaMap_{column_name}_mo/txt_files_input"```, in order for MetaMap to process your input text, a formating is necessary to remove non ascii and espace characters. We save the input text that MetaMap is using so that you can use the pos_info to read the full sentences. Files are saved using {unique_id}.txt.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/txt_files_output"``` is composed of files with all the candidates from MetaMap. Files are saved using {unique_id}.txt.

## Timeout

This section is not suitable for Windows OS.

Depending on the version of MetaMap you are using, and the text you are processing, you might find yourself stuck forever in an instance that is not producing anything. Similarly, you might want to spend a maximum amount of time to process a text and move on after a certain amount of time to the next one. 
For that reason, we developed a function called 'timeout_metamap_process'. You can run that function in the background using nohup or in another Python console. 

```python
from ParallelPyMetaMap import timeout_metamap_process

timeout_metamap_process()
```
timeout_metamap_process has two optional parameters:
- timeout: it has a default value of 10800 seconds. You need to input the maximun number of seconds you want ppmm to spend on one text. The value can not be lower than 300 seconds. 
- username: The default value is None. If you are on a shared machine, you can change None to your username on the machine so that timeout_metamap_process only looks and terminates your MetaMap process(es).

## Important
 [MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html) comes with a license. Please make sure you are following the terms of the licence when using MetaMap and its result. 

## Extra resources
[MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html)

[Data Sources](https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html)

[Documentation: Installation](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/Installation.html)

[Documentation: Semantic Types and Groups](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/SemanticTypesAndGroups.html)

[Documentation: Using MetaMap & options](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/UsingMetaMap.html)

[MetaMap Dataset](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/additional-tools/DataSetDownload.html)

## FAQ

Q: One or multiple of my processes are not sending results, what should I do?

A: You should first use the command 'top' (or similar command), in the result you should find a row called METAMAP(version).BINARY, if one of the process exceeded 3 hours, most likely the process will have difficulty to process this current row. You can choose to take the PID number of the process and then run on your terminal 'kill PID number'. When you do that the library will just ignore the current line and continue to the next one. Alternatively, you can use the timeout function explained above.

Q: How to determine the number of cores? 

A: There is no perfect answer to that question. Be mindful of the number of cores available on your machine. If you request more than present on your machine the code will not start. Please check first the number of cores on your machine and second the number of cores available before starting the process. If you are not careful with the number you selected, the machine might freeze and you will need to restart it. 

Q: Using the parameter restrict_to_sources, I receive a warning saying the data source is not present in my vocabulary, why that ? 

A: We do not know or check which version of MetaMap you are using, please read the documentation from [MetaMap Dataset](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/additional-tools/DataSetDownload.html) to see if the data source you requested is available for your installed version. 
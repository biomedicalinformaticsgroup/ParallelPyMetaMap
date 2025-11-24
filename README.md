# 🖥️🗺️ ParallelPyMetaMap
This code is to run MetaMap in parallel using Python.

## 📋 Requirements

To run the code, you will need a few things:

1) MetaMap installed locally.

2) You need to git clone the project and install it.

## ⚙️ Installation

ParallelPyMetaMap has several dependencies on other Python packages; it is recommended to install it in an isolated environment.

```bash
git clone https://github.com/biomedicalinformaticsgroup/ParallelPyMetaMap.git
pip install ./ParallelPyMetaMap
```

After installation, you can remove the file if you want, using:

`rm -rf ParallelPyMetaMap`

## 🚀 Get started
ParallelPyMetaMap utilises the number of cores available in your machine to create parallel instances of the MetaMap server to run multiple rows at the same time. To do so, ParallelPyMetaMap uses the multiprocessing library. 

ParallelPyMetaMap aims to annotate biomedical publications with the UMLS. For that, we are using a modified version of [PyMetamap](https://github.com/AnthonyMRios/pymetamap). We offer a wider range of options available in MetaMap, and extra information is extracted to a human-readable format during the process.

It was developed to be flexible and to automatically distribute the data to the number of cores you provide to ParallelPyMetaMap. Like Cadmus, dynamic generation is important. You can update your JSONs folder as your input grows without re-annotating previously annotated texts. In case something happened, you can just re-run the same code, and it will redistribute the missing documents to finish the annotations.

```python
from ParallelPyMetaMap import ppmm

if __name__ == "__main__":
    ppmm(NUMBER_OF_CORE(S)_TO_USE, 
        PATH_TO_METAMAP,
        file or path_to_file or cadmus or path_to_directory,
        column_name, 
        unique_id,
        machine_output or (fielded_mmi_output and extension_format)
        )
```

Mandatory parameters:
- number_of_core(s)_to_use: this is a mandatory parameter with no default value. Here, you need to insert the number of core(s) you want the function to use. 
- path_to_MetaMap: this is again a mandatory parameter with no default value. You need to insert the path to your MetaMap instance in the bin directory of MetaMap.
- column_name = 'content_text': this parameter has 'content_text' as the default value, which is the column that contains the text from cadmus. You need to input the name of the column where the text you want to annotate is stored. 
- unique_id = 'pmid': the default value is 'pmid'; again, this value has been chosen if your input is from cadmus. You need here to provide the name of the column that can act as a key. It must be unique, without spaces and no escape characters. 
- extension_format = None: this parameter depends on if you are running the code using fielded_mmi_output or machine_output. The default value is None; the None value will only work if you are running the code using machine_output. If you are using fielded_mmi_output, you need to decide if you want your annotated files to be structured like in the terminal, then extension_format = 'terminal', or if you prefer your files to be Python-like dictionaries, then extension_format = 'dict'. 
- file = None: None has a default value, you are required to only change file or cadmus or path_to_file or path_to directory from None. 'file' you can directly input your Pandas DataFrame.
- cadmus = None: If using a cadmus output you need to mention: abstract, content_text, pdf, html, xml, or plain_text in the column_name. Older and recent cadmus outputs are accepted.
- path_to_file = None: takes a string to the destination of the DataFrame saved in: .json, .p, .csv, .tsv, .json.zip.
- path_to_directory = None: takes a string to a directory of files to annotate. This directory must contain .txt and .txt.zip file formats. With this parameter, column_name will automatically be changed to 'text'.

Optional parameters:
- extension = 'txt': 'extension' has 'txt' as the default value. During the process, we save the annotation produced by MetaMap in a file; here, you can set the extension of these files. 
- verbose = False: MetaMap can show you the sentences while processing them. The default value is set to False. When set to False, the function only shows a summary of where the function is at. If you want to see the sentences being processed live, change the default to True.

You can find below the list of all the MetaMap options implemented in ParallelPyMetaMap with their associated default values:
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

You can find out more about these options on the National Library of Medicine website: [Documentation: Using MetaMap & options](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/UsingMetaMap.html). You will usually find the information about the options in the 'Usage Notes'.

## 🧮 What is the result?

The annotations from ParallelPyMetaMap are saved in a folder of zipped JSON files stored at ```'./output_ParallelPyMetaMap_{column_name}_{mmi (for fielded_mmi_output) or mo (for machine_output)}/annotated_json/{unique_id}.json.zip'```. You can open the files using the json and zipfile libraries an example is shown below. We also provide functions to open all the JSONs and collate the results in one Pandas DataFrame; more details later on the README.

```python
import zipfile
import json

with zipfile.ZipFile($PATH_TO_YOUR_FILE$, "r") as z:
    for filename in z.namelist():
        with z.open(filename) as f:
            d = f.read()
            content = json.loads(d)


f.close()
z.close()
```

## 💾 Fielded MMI Output Structure

To produce a Fielded MMI Output, you need to use the following structure of code:
```python
from ParallelPyMetaMap import ppmm

if __name__ == "__main__":
    ppmm(NUMBER_OF_CORE(S)_TO_USE, 
        PATH_TO_METAMAP,
        file or path_to_file or cadmus or path_to_directory,
        column_name, 
        unique_id,
        fielded_mmi_output = True,
        extension_format = ('dict' or 'terminal')
        )
```

The results for each processed text are saved in a zipped JSON file within the ```'./output_ParallelPyMetaMap_{column_name}_mmi/annotated_json/{unique_id}.json.zip'``` directory. Each JSON file, named ```{unique_id}.json.zip```, contains dictionaries where the key is the CUI (UMLS Concept Unique Identifier) and the values are the information associated with this CUI.
Each file contains:

- cui <class 'str'> - The CUI is the key for each dictionary within the file.
  - UMLS Concept Unique Identifier (CUI) – The CUI for the identified UMLS concept.
- umls_preferred_name <class 'str'>
  - The preferred name for the UMLS concept identified in the text. 
- semantic_type <class 'list'>
  - Comma-separated list of Semantic Type abbreviations for the identified UMLS concept. 
- full_semantic_type_name <class 'list'>
  - Comma-separated list of Semantic Type full names for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt)
- semantic_group_name <class 'list'>
  - Comma-separated list of Semantic Group full names for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt)
- occurrence <class 'numpy.int64'>
  - Number of times this CUI has been found in the text in total.
- annotation <class 'dict'>
  - You can find more about the annotation from the MetaMap documentation (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/MMI_Output.pdf). 

Other Outputs:
- ```"./output_ParallelPyMetaMap_{column_name}_mo/extra_resources"``` contains a {unique_id}_to_avoid.txt that keeps the {unique_id} of failed attempts to annotate a text due to either insufficient memory or that they exceed the timeout. It also has 2 zipped JSON Pandas DataFrames that have the Semantic Type full name and the Semantic Group full name.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/{extension_format}_files"``` is composed of zipped files with all the annotations from MetaMap. Files are saved using {unique_id}.{extension_format}.zip.

## 🖥️ Machine Output Structure

To produce a Machine Output, you need to use the following structure of code:
```python
from ParallelPyMetaMap import ppmm

if __name__ == "__main__":
    ppmm(NUMBER_OF_CORE(S)_TO_USE, 
        PATH_TO_METAMAP,
        file or path_to_file or cadmus or path_to_directory,
        column_name, 
        unique_id,
        machine_output = True
        )
```

The results for each processed text are saved in a zipped JSON file within the ```'./output_ParallelPyMetaMap_{column_name}_mo/annotated_json/{unique_id}.json.zip'``` directory. Each JSON file, named ```{unique_id}.json.zip```, contains dictionaries where the key is the CUI (UMLS Concept Unique Identifier) and the values are the information associated with this CUI.
Each file contains:

- cui <class 'str'> - The CUI is the key for each dictionary within the file.
  - UMLS Concept Unique Identifier (CUI) – The CUI for the identified UMLS concept.
- prefered_name <class 'str'>
  - The preferred name for the UMLS concept identified in the text. 
- semantic_type <class 'list'>
  - Comma-separated list of Semantic Type abbreviations for the identified UMLS concept. 
- full_semantic_type_name <class 'list'>
  - Comma-separated list of Semantic Type full names for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt)
- semantic_group_name <class 'list'>
  - Comma-separated list of Semantic Group full names for the identified UMLS concept. (https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt)
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
  - The score has a maximum value of 1000.00. The higher the score, the greater the relevance of the UMLS concept according to MetaMap. When the trigger was considered as negative/absent, the score is negative; in that case, the highest value is -1000.00.

Other Outputs:
- ```"./output_ParallelPyMetaMap_{column_name}_mo/extra_resources"``` contains a {unique_id}_to_avoid.txt that keeps the {unique_id} of failed attempts to annotate a text due to either insufficient memory or that they exceed the timeout. It also has 2 zipped JSON Pandas DataFrames that have the Semantic Type full name and the Semantic Group full name.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/{extension_format}_files_input"```, for MetaMap to process your input text, formatting is necessary to remove non-ASCII and escape characters. We save the input text that MetaMap is using so that you can use the pos_info to read the full sentences. Files are saved using {unique_id}.{extension_format}.zip.
- ```"./output_ParallelPyMetaMap_{column_name}_mo/{extension_format}_files_output"``` is composed of files with all the candidates from MetaMap. Files are saved using {unique_id}.{extension_format}.zip.

## ⌛ Timeout

This section is not suitable for the Windows OS.

Depending on the version of MetaMap you are using and the text you are processing, you might find yourself stuck forever in an instance that is not producing anything. Similarly, you might want to spend a maximum amount of time processing a text and move on after a certain amount of time to the next one. 
For that reason, we developed a function called 'timeout_metamap_process'. You can run that function in the background using nohup or in another Python console. 

```python
from ParallelPyMetaMap import timeout_metamap_process

timeout_metamap_process()
```
timeout_metamap_process has two optional parameters:
- timeout: it has a default value of 10800 seconds. You need to input the maximum number of seconds you want ppmm to spend on one text. The value cannot be lower than 300 seconds. 
- username: The default value is None. If you are on a shared machine, you can change None to your username on the machine so that timeout_metamap_process only looks for and terminates your MetaMap process(es).

## 🔄 Convert JSON files to DataFrame 

```python
from ParallelPyMetaMap import machine_out_json_to_df
from ParallelPyMetaMap import mmi_json_to_df

machine_out_json_to_df(path = './', filtering = [], previous_df = None)
mmi_json_to_df(path = './', filtering = [], previous_df = None)
```
The two functions above work the same way but handle different file structures. You will either use one or the other, depending on whether you used the 'machine_output' or the 'fielded_mmi_output' parameter while annotating your text.
Each function has three parameters:
- path: You need to provide the path to the directory where the JSONs are stored. For example, at the same level where you ran ppmm, you will need to input ```path = './output_ParallelPyMetaMap_{column_name}_{mmi (for fielded_mmi_output) or mo (for machine_output)}/annotated_json'```
- filtering: This parameter allows you to extract only part of the available information from the JSON files. In order to use this parameter, you need to input a list of strings from all the fields described in the previous section. You can also input the string 'id' into your list to keep the {unique_id} in the dataframe if you want to.
- previous_df: We like dynamic generation, and because of that, we know that new results can come and that we don't like to lose time re-running code that has already been processed. Here, you can input a previous DataFrame that you have obtained using this code, and we will only extract the information from the new files to add it to your previous DataFrame. Be careful, we don't save any results, so you will have to do it. 

## ⚠️ Important
 [MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html) comes with a license. Please make sure you are following the terms of the license when using MetaMap and its results. 

## 🧰 Extra resources

[MetaMap](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html)

[Data Sources](https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html)

[Documentation: Installation](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/Installation.html)

[Documentation: Semantic Types and Groups](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/SemanticTypesAndGroups.html)

[Documentation: Using MetaMap & options](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/UsingMetaMap.html)

[MetaMap Dataset](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/additional-tools/DataSetDownload.html)

## ❓ FAQ

Q: One or multiple of my processes are not sending results. What should I do?

A: You should first use the command 'top' (or a similar command), in the live result, you should find a row called METAMAP(version).BINARY, if one of the processes exceeded 3 hours, most likely the process will have difficulty processing the current row. You can choose to take the PID number of the process and then run it on your terminal, 'kill PID number'. When you do that, ppmm will just ignore the current row and continue to the next one. Alternatively, you can use the timeout function explained above.

Q: How to determine the number of cores? 

A: There is no perfect answer to that question. Be mindful of the number of cores available on your machine. If you request more than present on your machine, the code will not start. Please check first the number of cores on your machine, and second, the number of cores available before starting the process. If you are not careful with the number you selected, the machine might freeze, and you will need to restart the machine and ParallelPyMetaMap. 

Q: Using the parameter restrict_to_sources, I receive a warning saying the data source is not present in my vocabulary. Why that? 

A: We do not check which version of MetaMap you are using. Please read the documentation from [MetaMap Dataset](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/additional-tools/DataSetDownload.html) to see if the data source you requested is available for your installed version. 

Q: What is the performance of ParallelPyMetaMap?

ParallelPyMetaMap is just a Python wrapper for MetaMap, where extra information is added using other sources of information. We do not alter the entity recognition performance. To learn more about the performance of MetaMap, please refer to this [paper](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2995713/).

## 📦 Version

### Version 0.1.6
-> New input formats are implemented in this version:
  -> cadmus
  -> path to a directory

-> All the files we are creating are now zipped to save storage space. There is an automatic conversion of previous results in your next run.

-> The restart option is removed as restarting is now automatic as soon as the code is re-run.

-> path_to_file parameter accepts now the following file formats:
  -> .json
  -> .p (pickle object)
  -> .csv
  -> .tsv
  -> .json.zip

-> path_to_directory is parsing the following file formats:
  -> .txt
  -> .txt.zip

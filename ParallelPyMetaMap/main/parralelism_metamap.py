import multiprocessing as mp
import pandas as pd 
import numpy as np
from pathlib import Path
import pickle
import json
from datetime import datetime
import os
import glob
import zipfile

from ParallelPyMetaMap.altered_pymetamap.MetaMap import MetaMap
from ParallelPyMetaMap.altered_pymetamap.SubprocessBackend import SubprocessBackend
from ParallelPyMetaMap.main.output_files import output_files
from ParallelPyMetaMap.main.annotation_func import annotation_func
from ParallelPyMetaMap.main.df_semantictypes import df_semantictypes
from ParallelPyMetaMap.main.df_semgroups import df_semgroups
                      
def ppmm(numbers_of_cores,
        path_to_metamap,
        column_name = 'content_text',
        unique_id = 'pmid',
        extension = 'txt',
        extension_format = None,
        path_to_file = None,
        file = None,
        cadmus = None,
        path_to_directory = None,
        sep = ',',
        header = 'infer',
        usecols = None,
        nrows = None,
        index_col = None,
        skiprows = None,
        composite_phrase=4,
        fielded_mmi_output=False,
        machine_output=False,
        filename = None,
        file_format='sldi',
        allow_acronym_variants=False,
        word_sense_disambiguation=False,
        allow_large_n=False,
        strict_model=False,
        relaxed_model=False,
        allow_overmatches=False,
        allow_concept_gaps=False,
        term_processing=False,
        no_derivational_variants=False,
        derivational_variants=False,
        ignore_word_order=False,
        unique_acronym_variants=False,
        prefer_multiple_concepts=False,
        ignore_stop_phrases=False,
        compute_all_mappings=False,
        prune=False,
        mm_data_version=False,
        mm_data_year=False,
        verbose=False,
        exclude_sources=[],
        restrict_to_sources=[],
        restrict_to_sts=[],
        exclude_sts=[],
        no_nums=[]):

    if (fielded_mmi_output == False and machine_output == False) or (fielded_mmi_output == True and machine_output == True):
        print("You need to set either fielded_mmi_output or machine_output to 'True'")
        return None
        exit()
    
    if fielded_mmi_output == True:
        out_form = 'mmi'
    else:
        out_form = 'mo'
    
    if type(path_to_directory) == str:
        column_name = 'text'
        unique_id = 'index_id'

    output_files(column_name, out_form, extension)

    if numbers_of_cores >= mp.cpu_count():
        print('The number of cores you want to use is equal or greater than the numbers of cores in your machine. We stop the script now')
        return None
        exit()
    else:
        par_core = numbers_of_cores
    
    NoneType = type(None)
    number_of_input = 0
    for i in [type(path_to_file), type(file), type(cadmus), type(path_to_directory)]:
        if i != NoneType:
            number_of_input += 1

    if number_of_input != 1: 
        print('You need to input either a path to a json file, a Pandas DataFrame, a cadmus output path or a directory to .txt files. You can not input more than one.')
        return None
        exit()
    elif type(path_to_file) == str and path_to_file[-5] == '.json':
        f = open(path_to_file)
        data = json.load(f)
        f.close()
        df = pd.read_json(data, orient='index')
    elif type(path_to_file) == str and path_to_file[-2] == '.p':
        df = pickle.load(open('path_to_file','rb'))
    elif type(path_to_file) == str and (path_to_file[-4] == '.csv' or path_to_file[-4] == '.tsv'):
        df = pd.read_csv(path_to_file, sep, header, usecols, nrows, index_col, skiprows)
    elif type(path_to_file) == str and path_to_file[-9] == '.json.zip':
        with zipfile.ZipFile(path_to_file, "r") as z:
            for path_filename in z.namelist():
                with z.open(path_filename) as f:
                    data = f.read()
                    data = json.loads(data)
        f.close()
        z.close()
        df = pd.read_json(data, orient='index')
        if 'pmid' in df.columns:
            df.pmid = df.pmid.astype(str)
    elif type(file) == pd.core.frame.DataFrame:
        df = file
    elif type(cadmus) == str:
        if len(glob.glob(f'{cadmus}/retrieved_df/*.json')) > 0:
            f = open(f'{cadmus}/retrieved_df/retrieved_df2.json')
            data = json.load(f)
            f.close()
            df = pd.read_json(data, orient='index')
        elif len(glob.glob(f'{cadmus}/retrieved_df/*.zip')) > 0:
            with zipfile.ZipFile(f'{cadmus}/retrieved_df/retrieved_df2.json.zip', "r") as z:
                for cad_filename in z.namelist():
                    with z.open(cad_filename) as f:
                        data = f.read()
                        data = json.loads(data)
            f.close()
            z.close()
            df = pd.read_json(data, orient='index')
            df.pmid = df.pmid.astype(str)
        else:
            print('PPMM is not able to find retrieved_df2 in your cadmus directory.')
            return None
            exit()
        if column_name == 'abstract':
            pass
        else:
            df = df[df[f'{column_name}'] == 1]
            df = df[[f'{column_name}', f'{unique_id}']]
            path_to_cadmus_files = []
            if column_name == 'content_text':
                all_content = glob.glob(f'{cadmus}/retrieved_parsed_files/{column_name}/*')
                for i in range(len(df)):
                    for j in range(len(all_content)):
                        if df.index[i] in all_content[j]:
                            if len(glob.glob(f'{cadmus}/retrieved_parsed_files/{column_name}/{str(df.index[i]) + str(".txt")}')) > 0:
                                path_to_cadmus_files.append(str(cadmus) + str(f'/retrieved_parsed_files/{column_name}/') + str(df.index[i]) + str('.txt'))
                            elif len(glob.glob(f'{cadmus}/retrieved_parsed_files/{column_name}/{str(df.index[i]) + str(".txt.zip")}')) > 0:
                                path_to_cadmus_files.append(str(cadmus) + str(f'/retrieved_parsed_files/{column_name}/') + str(df.index[i]) + str('.txt.zip'))

            else:
                for i in range(len(df)):
                    if len(glob.glob(f'{cadmus}/retrieved_parsed_files/{column_name}s/{str(df.index[i]) + str(".txt")}')) > 0:
                        path_to_cadmus_files.append(str(cadmus) + str(f'/retrieved_parsed_files/{column_name}s/') + str(df.index[i]) + str('.txt'))
                    elif len(glob.glob(f'{cadmus}/retrieved_parsed_files/{column_name}s/{str(df.index[i]) + str(".txt.zip")}')) > 0:
                        path_to_cadmus_files.append(str(cadmus) + str(f'/retrieved_parsed_files/{column_name}s/') + str(df.index[i]) + str('.txt.zip'))
            df['file_path'] = path_to_cadmus_files
    
    elif type(path_to_directory) == str:
        if len(glob.glob(f'{path_to_directory}/*.txt', recursive=True)) > 0:
            all_txts = glob.glob(f'{path_to_directory}/*.txt', recursive=True)
        elif len(glob.glob(f'{path_to_directory}/*.txt.zip', recursive=True)) > 0:
            all_txts = glob.glob(f'{path_to_directory}/*.txt.zip', recursive=True)
        text = []
        index = []
        path_to_directory_files = []
        for i in range(len(all_txts)):
            text.append(1)
            index.append(all_txts[i].split('/')[-1][:-4])
            path_to_directory_files.append(all_txts[i])
        df = pd.DataFrame(list(zip(text, index, path_to_directory_files)),
                columns =['text', 'index_id', 'file_path'])
    else:
        print('You did not input any correct data format to process')
        return None
        exit()

    if extension_format == 'dict' or extension_format == 'terminal' or extension_format == None:
        pass
    else:
        print("Your extension_format parameter should be equal to 'dict' or 'terminal' for mmi output or 'None' for mo output please enter a valid parameter.")
        return None
        exit()
    
    if fielded_mmi_output == True:
        if extension_format == 'dict' or extension_format == 'terminal':
            pass
        else:
            print("You are running the your code with the 'fielded_mmi_output' parameter please change the 'extension_format' to 'dict' or 'terminal'.")
            return None
            exit()
    else:
        if extension_format == None:
            pass
        else:
            print("You are running the your code with the 'machine_output' parameter please change 'extension_format' to 'None'.")
            return None
            exit()

    if len(df) < par_core:
            par_core = len(df)
    
    if cadmus == None and path_to_directory == None:
        pmids = []
        for i in range(len(df)):
            if df[column_name][i] != df[column_name][i] or df[column_name][i] == None or df[column_name][i] == '' or df[column_name][i][:4] == 'ABS:':
                pass
            else:
                try:
                    if len(df[column_name][i].split()) > 150000:
                        pass
                    else:
                        pmids.append(df.iloc[i][unique_id])
                except:
                    pmids.append(df.iloc[i][unique_id])
            if df.iloc[i][unique_id] != df.iloc[i][unique_id] or df.iloc[i][unique_id] == None or df.iloc[i][unique_id] == '':
                print('Your unique identifier is empty/None/NaN, please choose a unique identifier present for each row.')
                return None
                exit()
            if '/' in str(df.iloc[i][unique_id]):
                print('Your unique identifier contains "/" please choose another unique identifier, remove the "/" from the current unique identifier or replace "/" with another character.')
                return None
                exit()
        if len(np.unique(pmids)) == len(pmids):
            df = df[df[unique_id].isin(pmids)]
        else:
            print('It seems that one of your unique identifier is duplicate, please choose a unique identifier present for each row.')
            return None
            exit()

    update = False

    retrieved_path = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json/*.json.zip')
    if len(retrieved_path) == 0:
        update = False
    elif retrieved_path[0]:
        update = True
    else:
        update = False

    if update == True:

        list_original_index = []
        for i in range(len(df)):
            list_original_index.append(df.iloc[i][unique_id])

        list_check_index = []
        for i in range(len(retrieved_path)):
            list_check_index.append(retrieved_path[i].split('/')[-1].split('.')[0])

        list_check_index = list(np.unique(list_check_index))

        list_to_do = list(set(list_original_index) - set(list_check_index))

        file_avoid = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/{unique_id}_to_avoid.txt", 'r')
        Lines = file_avoid.readlines()
        list_avoid = []
        for line in Lines:
            list_avoid.append(line.strip())

        list_to_do = list(set(list_to_do) - set(list_avoid))

        subset_list_update = list_to_do

        df = df[df[unique_id].isin(subset_list_update)]
        if len(df) == 0:
            print('No new row since last time.')
            return None
            exit()
        if len(df) < par_core:
            par_core = len(df)

    else:
        print(str('Now creating ') + str(f"output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/{unique_id}_to_avoid.txt"))
        f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/{unique_id}_to_avoid.txt", "a")
        f.close()
        print(str('Now creating ') + str(f"output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.json.zip"))
        df_semantictypes(column_name, out_form)
        print(str('Now creating ') + str(f"output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.json.zip"))
        df_semgroups(column_name, out_form)


    mm = MetaMap.get_instance(path_to_metamap)

    if par_core > 1:
        data = []
        for i in range(par_core):
            if i == 0:
                current_data = (df[:round(len(df)/par_core)], i+1, mm, cadmus, path_to_directory, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            elif i > 0 and i+1 != par_core:
                current_data = (df[(round(len(df)/par_core))*i:(round(len(df)/par_core))*(i+1)], i+1, mm, cadmus, path_to_directory, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            else:
                current_data = (df[round(len(df)/par_core)*i:], i+1, mm, cadmus, path_to_directory, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
    else:
        data = [(df, 1, mm, cadmus, path_to_directory, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)]

    with mp.Pool(numbers_of_cores) as pool:
        pool.starmap(annotation_func, data)
    
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Process complete'))
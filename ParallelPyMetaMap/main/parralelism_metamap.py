import multiprocessing as mp
import pandas as pd 
import numpy as np
from pathlib import Path
import pickle
from datetime import datetime

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
        extension_format = 'terminal',
        path_to_file = None,
        file = None,
        composite_phrase=4,
        filename=None,
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
        verbose=False,
        exclude_sources=[],
        restrict_to_sources=[],
        restrict_to_sts=[],
        exclude_sts=[],
        no_nums=[]):

    output_files(column_name, extension)

    if numbers_of_cores >= mp.cpu_count():
        print('The number of cores you want to use is equal or greater than the numbers of cores in your machine. We stop the script now')
        return None
        exit()
    elif numbers_of_cores < 4:
        par_core = 1
    elif numbers_of_cores > 3:
        par_core = (numbers_of_cores - 2)

    if path_to_file != None and file != None:
        print('You need to input either a path to a Pickle object or a Pandas DataFrame. You can not input both!')
        return None
        exit()
    elif path_to_file != None:
        df = pickle.load(open(path_to_file, 'rb'))
    elif type(file) == pd.core.frame.DataFrame:
        df = file
    else:
        print('You did not input any data to process')
        return None
        exit()

    if extension_format == 'dict' or extension_format == 'terminal':
        pass
    else:
        print("Your extension_format parameter should be equal to 'dict' or 'terminal' please enter a valid parameter.")
        return None
        exit()

    if len(df) < par_core:
            par_core = len(df)
    
    pmids = []
    for i in range(len(df)):
        if df[column_name][i] != df[column_name][i] or df[column_name][i] == None or df[column_name][i] == '' or df[column_name][i][:4] == 'ABS:':
            pass
        else:
            if len(df.iloc[i][column_name].split()) > 150000:
                pass
            else:
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

    retrieved_path = [path for path in Path(f'output_ParallelPyMetaMap_{column_name}/annotated_df').iterdir() if path.stem == f'annotated_{column_name}_{unique_id}_df2']
    if len(retrieved_path) == 0:
        update = False
    elif retrieved_path[0]:
        update = True
    else:
        update = False


    if update == True:

        df_processed = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'rb'))

        list_original_index = []
        for i in range(len(df)):
            list_original_index.append(df.iloc[i][unique_id])

        list_check_index = []
        list_check_index = list(np.unique(df_processed[unique_id]))

        list_to_do = list(set(list_original_index) - set(list_check_index))

        file_avoid = open(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/{unique_id}_to_avoid.txt", 'r')
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
        print(str('Now creating ') + str(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/{unique_id}_to_avoid.txt"))
        f = open(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/{unique_id}_to_avoid.txt", "a")
        f.close()
        print(str('Now creating ') + str(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/df_semantictypes.p"))
        df_semantictypes(column_name)
        print(str('Now creating ') + str(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/df_semgroups.p"))
        df_semgroups(column_name)


    mm = MetaMap.get_instance(path_to_metamap)

    if par_core > 1:
        data = []
        for i in range(par_core):
            if i == 0:
                current_data = (df[:round(len(df)/par_core)], i+1, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            elif i > 0 and i+1 != par_core:
                current_data = (df[(round(len(df)/par_core))*i:(round(len(df)/par_core))*(i+1)], i+1, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            else:
                current_data = (df[round(len(df)/par_core)*i:], i+1, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
    else:
        data = [(df, 1, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)]

    with mp.Pool(numbers_of_cores) as pool:
        pool.starmap(annotation_func, data)
    
    concat_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_1.p', 'rb'))
    if par_core > 1:
        for i in range(1,par_core):
            df_dynamic = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_{i+1}.p', 'rb'))
            concat_df = pd.concat([concat_df, df_dynamic])


    pickle.dump(concat_df, open(f'./output_ParallelPyMetaMap_{column_name}/annotated_df/annotated_{column_name}_{unique_id}_df.p', 'wb'))
    
    if update == True:
        final_df = pd.concat([df_processed, concat_df])
    else:
        final_df = concat_df

    pickle.dump(final_df, open(f'./output_ParallelPyMetaMap_{column_name}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'wb'))

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Process complete'))
import multiprocessing as mp
import pandas as pd 
import numpy as np
from pathlib import Path
import pickle
from datetime import datetime
import os
import glob

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
        #restart = False,
        path_to_file = None,
        file = None,
        composite_phrase=4,
        fielded_mmi_output=False,
        machine_output=False,
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

    output_files(column_name, out_form, extension)

    if numbers_of_cores >= mp.cpu_count():
        print('The number of cores you want to use is equal or greater than the numbers of cores in your machine. We stop the script now')
        return None
        exit()
    else:
        par_core = numbers_of_cores
    '''elif numbers_of_cores < 4:
        par_core = 1
    elif numbers_of_cores > 3:
        par_core = (numbers_of_cores - 2)'''

    if path_to_file != None and file != None:
        print('You need to input either a path to a Pickle object or a Pandas DataFrame. You can not input both.')
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

    retrieved_path = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json/*.json')
    if len(retrieved_path) == 0:
        update = False
    elif retrieved_path[0]:
        update = True
    else:
        update = False
    
    '''if restart == True and len([name for name in os.listdir(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/') if os.path.isfile(os.path.join(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/', name))]) == 0:
        print('There is/are no temporary_df(s) in the directory. The code never started to annotate. Please change the "restart" parameter to "False". You might want to check if you get another error.')
        return None
        exit()
    
    if restart == False:
        pass
    else:
        needed_restart = False
        concat_df = None
        if update == True:

            if fielded_mmi_output == True:  
                df_processed = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'rb'))
                df_processed = df_processed[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation', f'{unique_id}']]
            if machine_output == True:
                df_processed = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'rb'))
                df_processed = df_processed[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]

            count_temp_files = len([name for name in os.listdir(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/') if os.path.isfile(os.path.join(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/', name))])
            if count_temp_files > 1:
                    for i in range(count_temp_files):
                        df_dynamic = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/annotated_{column_name}_df2_{i+1}.p', 'rb'))
                        if fielded_mmi_output == True:
                            if df_dynamic.shape[1] == 8:
                                df_dynamic = df_dynamic[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation', f'{unique_id}']]
                                df_processed = pd.concat([df_processed, df_dynamic])
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])
                            else:
                                needed_restart = True
                                
                                df_dynamic['semantic_type'] = df_dynamic['semantic_type'].str.strip('[]').str.split(',')

                                df_semantictypes_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.p', 'rb'))
                                df_semgroups_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.p', 'rb'))

                                full_semantic_type_name_list = []
                                for i in range(len(df_dynamic)):
                                    full_semantic_type_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        full_semantic_type_name_list_current.append(df_semantictypes_df[df_semantictypes_df.abbreviation == df_dynamic.iloc[i].semantic_type[j]].full_semantic_type_name.values[0])
                                    full_semantic_type_name_list.append(full_semantic_type_name_list_current)
                                df_dynamic["full_semantic_type_name"] = full_semantic_type_name_list

                                semantic_group_name_list = []
                                for i in range(len(df_dynamic)):
                                    semantic_group_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        semantic_group_name_list_current.append(df_semgroups_df[df_semgroups_df.full_semantic_type_name == df_dynamic.iloc[i].full_semantic_type_name[j]].semantic_group_name.values[0])
                                    semantic_group_name_list.append(semantic_group_name_list_current)
                                df_dynamic["semantic_group_name"] = semantic_group_name_list
                                df_dynamic = df_dynamic[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation', f'{unique_id}']]
                                df_processed = pd.concat([df_processed, df_dynamic])
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])
                        if machine_output == True:
                            if df_dynamic.shape[1] == 12:
                                df_dynamic = df_dynamic[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]
                                df_processed = pd.concat([df_processed, df_dynamic])
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])
                            else:
                                needed_restart = True

                                df_dynamic = df_dynamic.drop_duplicates(subset=['cui', 'trigger', 'pos_info', f'{unique_id}'])
                                df_dynamic = df_dynamic.reset_index(drop=True)
                                df_dynamic['pos_info'] = df_dynamic['pos_info'].str.strip('[]').str.split(',')
                                aggregation_functions = {'occurrence': 'sum', 'negation': 'sum', 'sab': lambda x: list(x), 'trigger': lambda x: list(x), 'score': lambda x: list(x), 'pos_info': lambda x: list(x), 'prefered_name': 'first', 'semantic_type': 'first'}
                                df_dynamic = df_dynamic.groupby(['cui', f'{unique_id}']).aggregate(aggregation_functions)
                                df_dynamic = df_dynamic.reset_index()
                                
                                df_semantictypes_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.p', 'rb'))
                                df_semgroups_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.p', 'rb'))

                                full_semantic_type_name_list = []
                                for i in range(len(df_dynamic)):
                                    full_semantic_type_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        full_semantic_type_name_list_current.append(df_semantictypes_df[df_semantictypes_df.abbreviation == df_dynamic.iloc[i].semantic_type[j]].full_semantic_type_name.values[0])
                                    full_semantic_type_name_list.append(full_semantic_type_name_list_current)
                                df_dynamic["full_semantic_type_name"] = full_semantic_type_name_list

                                semantic_group_name_list = []
                                for i in range(len(df_dynamic)):
                                    semantic_group_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        semantic_group_name_list_current.append(df_semgroups_df[df_semgroups_df.full_semantic_type_name == df_dynamic.iloc[i].full_semantic_type_name[j]].semantic_group_name.values[0])
                                    semantic_group_name_list.append(semantic_group_name_list_current)
                                df_dynamic["semantic_group_name"] = semantic_group_name_list
                                df_dynamic = df_dynamic[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]
                                df_processed = pd.concat([df_processed, df_dynamic])
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])


            df_processed = df_processed.drop_duplicates(subset=[f'{unique_id}', 'cui'], keep='first')
            df_processed = df_processed.reset_index(drop=True)
            
            concat_df = concat_df.drop_duplicates(subset=[f'{unique_id}', 'cui'], keep='first')
            concat_df = concat_df.reset_index(drop=True)        
            if needed_restart == False:
                print('The process seems to be done already, please set the restart parameter to "False"')
                return None
                exit()
            else:
                pickle.dump(concat_df, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df/annotated_{column_name}_{unique_id}_df.p', 'wb'))
                pickle.dump(df_processed, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'wb'))
        else:
            count_temp_files = len([name for name in os.listdir(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/') if os.path.isfile(os.path.join(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/', name))])
            if count_temp_files > 1:
                    for i in range(count_temp_files):
                        df_dynamic = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/annotated_{column_name}_df2_{i+1}.p', 'rb'))
                        if fielded_mmi_output == True:
                            if df_dynamic.shape[1] == 8:
                                df_dynamic = df_dynamic[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation', f'{unique_id}']]
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])
                            else:
                                df_dynamic['semantic_type'] = df_dynamic['semantic_type'].str.strip('[]').str.split(',')

                                df_semantictypes_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.p', 'rb'))
                                df_semgroups_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.p', 'rb'))

                                full_semantic_type_name_list = []
                                for i in range(len(df_dynamic)):
                                    full_semantic_type_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        full_semantic_type_name_list_current.append(df_semantictypes_df[df_semantictypes_df.abbreviation == df_dynamic.iloc[i].semantic_type[j]].full_semantic_type_name.values[0])
                                    full_semantic_type_name_list.append(full_semantic_type_name_list_current)
                                df_dynamic["full_semantic_type_name"] = full_semantic_type_name_list

                                semantic_group_name_list = []
                                for i in range(len(df_dynamic)):
                                    semantic_group_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        semantic_group_name_list_current.append(df_semgroups_df[df_semgroups_df.full_semantic_type_name == df_dynamic.iloc[i].full_semantic_type_name[j]].semantic_group_name.values[0])
                                    semantic_group_name_list.append(semantic_group_name_list_current)
                                df_dynamic["semantic_group_name"] = semantic_group_name_list
                                df_dynamic = df_dynamic[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation', f'{unique_id}']]
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])
                        if machine_output == True:
                            if df_dynamic.shape[1] == 12:
                                df_dynamic = df_dynamic[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]
                                df_processed = pd.concat([df_processed, df_dynamic])
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])
                            else:

                                df_dynamic = df_dynamic.drop_duplicates(subset=['cui', 'trigger', 'pos_info', f'{unique_id}'])
                                df_dynamic = df_dynamic.reset_index(drop=True)
                                df_dynamic['pos_info'] = df_dynamic['pos_info'].str.strip('[]').str.split(',')
                                aggregation_functions = {'occurrence': 'sum', 'negation': 'sum', 'sab': lambda x: list(x), 'trigger': lambda x: list(x), 'score': lambda x: list(x), 'pos_info': lambda x: list(x), 'prefered_name': 'first', 'semantic_type': 'first'}
                                df_dynamic = df_dynamic.groupby(['cui', f'{unique_id}']).aggregate(aggregation_functions)
                                df_dynamic = df_dynamic.reset_index()

                                df_semantictypes_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.p', 'rb'))
                                df_semgroups_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.p', 'rb'))

                                full_semantic_type_name_list = []
                                for i in range(len(df_dynamic)):
                                    full_semantic_type_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        full_semantic_type_name_list_current.append(df_semantictypes_df[df_semantictypes_df.abbreviation == df_dynamic.iloc[i].semantic_type[j]].full_semantic_type_name.values[0])
                                    full_semantic_type_name_list.append(full_semantic_type_name_list_current)
                                df_dynamic["full_semantic_type_name"] = full_semantic_type_name_list

                                semantic_group_name_list = []
                                for i in range(len(df_dynamic)):
                                    semantic_group_name_list_current = []
                                    for j in range(len(df_dynamic.iloc[i].semantic_type)): 
                                        semantic_group_name_list_current.append(df_semgroups_df[df_semgroups_df.full_semantic_type_name == df_dynamic.iloc[i].full_semantic_type_name[j]].semantic_group_name.values[0])
                                    semantic_group_name_list.append(semantic_group_name_list_current)
                                df_dynamic["semantic_group_name"] = semantic_group_name_list
                                df_dynamic = df_dynamic[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]
                                if type(concat_df) == None:
                                    concat_df = df_dynamic
                                else:
                                    concat_df = pd.concat([concat_df, df_dynamic])


            concat_df = concat_df.drop_duplicates(subset=[f'{unique_id}', 'cui'], keep='first')
            concat_df = concat_df.reset_index(drop=True)        
            pickle.dump(concat_df, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df/annotated_{column_name}_{unique_id}_df.p', 'wb'))
            pickle.dump(concat_df, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'wb'))
            update = True'''

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
        print(str('Now creating ') + str(f"output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.p"))
        df_semantictypes(column_name, out_form)
        print(str('Now creating ') + str(f"output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.p"))
        df_semgroups(column_name, out_form)


    mm = MetaMap.get_instance(path_to_metamap)

    if par_core > 1:
        data = []
        for i in range(par_core):
            if i == 0:
                current_data = (df[:round(len(df)/par_core)], i+1, mm, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            elif i > 0 and i+1 != par_core:
                current_data = (df[(round(len(df)/par_core))*i:(round(len(df)/par_core))*(i+1)], i+1, mm, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            else:
                current_data = (df[round(len(df)/par_core)*i:], i+1, mm, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
    else:
        data = [(df, 1, mm, column_name, out_form, unique_id, extension, extension_format, composite_phrase, fielded_mmi_output,
                        machine_output, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, mm_data_year, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)]

    with mp.Pool(numbers_of_cores) as pool:
        pool.starmap(annotation_func, data)
    
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Process complete'))
import multiprocessing as mp
import pandas as pd 
import numpy as np
from pathlib import Path
import pickle
from collections import defaultdict
import random
from datetime import datetime
from pathlib import Path
import os

from ParallelPyMetaMap.src.altered_pymetamap import MetaMap
from ParallelPyMetaMap.src.altered_pymetamap import SubprocessBackend

class bold:
   BEGIN = '\033[1m'
   END = '\033[0m'

def output_files(column_name, extension):
    #creating the directories we are planning on using the save the result of the system
    for path in [f'output_ParallelPyMetaMap_{column_name}',
                f'output_ParallelPyMetaMap_{column_name}/annotated_df',
                f'output_ParallelPyMetaMap_{column_name}/temporary_df',
                f'output_ParallelPyMetaMap_{column_name}/to_avoid',
                f'output_ParallelPyMetaMap_{column_name}/{extension}_files'
                ]:
        try:
            #try to create the directory, most likely will work for new project
            os.mkdir(path)
            print(f'Now creating {path}')
        except:
            #if the directory already exist just pass
            pass

def removeNonAscii(s):
    return "".join(filter(lambda x: ord(x)<128, s))

def concept2dict(concepts):
    d = defaultdict(list)
    for c in concepts:
        d[c.index].append(c._asdict())
    return d

def annotation_func(df, 
                    batch, 
                    batch_size, 
                    mm,
                    column_name,
                    unique_id,
                    extension,
                    extension_format,
                    composite_phrase,
                    filename,
                    file_format,
                    allow_acronym_variants,
                    word_sense_disambiguation,
                    allow_large_n,
                    strict_model,
                    relaxed_model,
                    allow_overmatches,
                    allow_concept_gaps,
                    term_processing,
                    no_derivational_variants,
                    derivational_variants,
                    ignore_word_order,
                    unique_acronym_variants,
                    prefer_multiple_concepts,
                    ignore_stop_phrases,
                    compute_all_mappings,
                    prune,
                    mm_data_version,
                    verbose,
                    exclude_sources,
                    restrict_to_sources,
                    restrict_to_sts,
                    exclude_sts,
                    no_nums):

    list_of_semtypes = []
    list_of_cuis = []
    list_of_preferred_names = []
    list_of_annotations = []
    idx = []
    occurence = []
    negation = []
    for j in range(len(df[column_name])):
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print(str(current_time) + str(' We are at row ') + str(j+1) + str(' out of ') + str(len(df)) + str(' from proccess ') + bold.BEGIN + str(batch) + bold.END)
        print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str(round((float(j)/float(len(df)))*100, 2)) + str('%') + bold.END)
        print(str(current_time) + str(' Processing ') + str(df.iloc[j][unique_id]))
        if df[column_name][j] != df[column_name][j] or df[column_name][j] == None or df[column_name][j] == '' or df[column_name][j][:4] == 'ABS:':
            pass
        else:
            term = removeNonAscii(df[column_name][j])
            term = [term]
            ids = list(range(len(term)))
            concepts, error = mm.extract_concepts(term, 
                                                 ids, 
                                                 composite_phrase,
                                                 filename,
                                                 file_format,
                                                 allow_acronym_variants,
                                                 word_sense_disambiguation,
                                                 allow_large_n,
                                                 strict_model,
                                                 relaxed_model,
                                                 allow_overmatches,
                                                 allow_concept_gaps,
                                                 term_processing,
                                                 no_derivational_variants,
                                                 derivational_variants,
                                                 ignore_word_order,
                                                 unique_acronym_variants,
                                                 prefer_multiple_concepts,
                                                 ignore_stop_phrases,
                                                 compute_all_mappings,
                                                 prune,
                                                 mm_data_version,
                                                 verbose,
                                                 exclude_sources,
                                                 restrict_to_sources,
                                                 restrict_to_sts,
                                                 exclude_sts,
                                                 no_nums)
            data = concept2dict(concepts)
            text = str
            neg = 0

            for i in range(len(data[str(0)])):
                if 'preferred_name' and 'cui' in data[str(0)][i]:
                    if 'semtypes' in data[str(0)][i]:
                        list_of_semtypes.append(data[str(0)][i]['semtypes'])
                    else:
                        list_of_semtypes.append(None)
                    list_of_preferred_names.append(data[str(0)][i]['preferred_name'])
                    list_of_cuis.append(data[str(0)][i]['cui'])
                    list_of_annotations.append(data[str(0)][i])
                    text = data[str(0)][i].get('trigger')
                    neg = (int(str(text).count('noun-1')) + int(str(text).count('adj-1')) + int(str(text).count('verb-1')) + int(str(text).count('adv-1')) + int(str(text).count('integer-1')) + int(str(text).count('numeral-1')) + int(str(text).count('prep-1')) + int(str(text).count('number-1')) + int(str(text).count('percentage-1')) + int(str(text).count('conj-1')) + int(str(text).count('ordinal-1')) + int(str(text).count('UNKNOWN-1')) + int(str(text).count('aux-1')) + int(str(text).count('fraction-1')))
                    negation.append(neg)
                    occurence.append(neg + (int(str(text).count('noun-0')) + int(str(text).count('adj-0')) + int(str(text).count('verb-0')) + int(str(text).count('adv-0')) + int(str(text).count('integer-0')) + int(str(text).count('numeral-0')) + int(str(text).count('prep-0')) + int(str(text).count('number-0')) + int(str(text).count('percentage-0')) + int(str(text).count('conj-0')) + int(str(text).count('ordinal-0')) + int(str(text).count('UNKNOWN-0')) + int(str(text).count('aux-0')) + int(str(text).count('fraction-0'))))
                    idx.append(df.iloc[j][unique_id])
                    if extension_format == 'dict':
                        f = open(f"./output_ParallelPyMetaMap_{column_name}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str(str(data[str(0)][i]) + ('\n')))
                        f.close()
                    elif extension_format == 'terminal':
                        f = open(f"./output_ParallelPyMetaMap_{column_name}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str('USER|') + str(data[str(0)][i].get('mm')) + str('|') + str(data[str(0)][i].get('score')) + str('|') + str(data[str(0)][i].get('preferred_name')) + str('|') + str(data[str(0)][i].get('cui')) + str('|') + str(data[str(0)][i].get('semtypes')) + str('|') + str(data[str(0)][i].get('trigger')) + str('|') + str(data[str(0)][i].get('location')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('|') + str(data[str(0)][i].get('tree_codes')) + str('\n')) 
                        f.close()

                if 'aa' in data[str(0)][i]:
                    if extension_format == 'dict':
                        f = open(f"./output_ParallelPyMetaMap_{column_name}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str(str(data[str(0)][i]) + ('\n')))
                        f.close()
                    elif extension_format == 'terminal':
                        f = open(f"./output_ParallelPyMetaMap_{column_name}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str('USER|') + str(data[str(0)][i].get('aa')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('\n')) 
                        f.close()

                if 'ua' in data[str(0)][i]:
                    if extension_format == 'dict':
                        f = open(f"./output_ParallelPyMetaMap_{column_name}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str(str(data[str(0)][i]) + ('\n')))
                        f.close()
                    elif extension_format == 'terminal':
                        f = open(f"./output_ParallelPyMetaMap_{column_name}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str('USER|') + str(data[str(0)][i].get('ua')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('\n')) 
                        f.close()

        if (j % 100 == 0 and j != 0):
            now = datetime.now()
            current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
            print(str(current_time) + str(' Saving file'))
            annotated_df = pd.DataFrame(
                {'semantic_type': list_of_semtypes,
                'umls_preferred_name': list_of_preferred_names,
                'occurence': occurence,
                'negation': negation,
                'cui': list_of_cuis,
                'annotation': list_of_annotations,
                f'{unique_id}' : idx
                })
            pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
                
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str(round((float(j+1)/float(len(df)))*100, 2)) + str('%') + bold.END)
    print(str(current_time) + str(' Saving file'))
    annotated_df = pd.DataFrame(
        {'semantic_type': list_of_semtypes,
        'umls_preferred_name': list_of_preferred_names,
        'occurence': occurence,
        'negation': negation,
        'cui': list_of_cuis,
        'annotation': list_of_annotations,
        f'{unique_id}' : idx
        })
    pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
                      
def parralelism_metamap(numbers_of_cores,
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
        return 'The number of cores you want to use is equal or greater than the numbers of cores in your machine. We stop the script now'
        exit()
    elif numbers_of_cores < 4:
        par_core = 1
    elif numbers_of_cores > 3:
        par_core = (numbers_of_cores - 2)

    if path_to_file != None and file != None:
        return 'You need to input either a path to a Pickle object or a Pandas DataFrame. You can not input both!'
        exit()
    elif path_to_file != None:
        df = pickle.load(open(path_to_file, 'rb'))
    elif type(file) == pd.core.frame.DataFrame:
        df = file
    else:
        return 'You did not input any data to process'
        exit()

    if extension_format == 'dict' or extension_format == 'terminal':
        pass
    else:
        return "Your extension_format parameter should be equal to 'dict' or 'terminal' please enter a valid parameter."
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

        file_avoid = open(f"./output_ParallelPyMetaMap_{column_name}/to_avoid/{unique_id}_to_avoid.txt", 'r')
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
        if len(df) < par_core:
            par_core = len(df)
    else:
        f = open(f"./output_ParallelPyMetaMap_{column_name}/to_avoid/{unique_id}_to_avoid.txt", "a")
        f.close()

    mm = MetaMap.get_instance(path_to_metamap)

    df_size = len(df[column_name])
    batch_size = round(len(df)/par_core)

    if par_core > 1:
        data = []
        for i in range(par_core):
            if i == 0:
                current_data = (df[:round(len(df)/par_core)], i+1, batch_size, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            elif i > 0 and i+1 != par_core:
                current_data = (df[(round(len(df)/par_core))*i:(round(len(df)/par_core))*(i+1)], i+1, batch_size, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
            else:
                current_data = (df[round(len(df)/par_core)*i:], i+1, batch_size, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)
                data.append(current_data)
    else:
        data = [(df, 1, batch_size, mm, column_name, unique_id, extension, extension_format, composite_phrase, filename, file_format, allow_acronym_variants, word_sense_disambiguation,
                        allow_large_n, strict_model, relaxed_model, allow_overmatches, allow_concept_gaps, term_processing, no_derivational_variants,
                        derivational_variants, ignore_word_order, unique_acronym_variants, prefer_multiple_concepts, ignore_stop_phrases, compute_all_mappings,
                        prune, mm_data_version, verbose, exclude_sources, restrict_to_sources, restrict_to_sts, exclude_sts, no_nums)]

    with mp.Pool(numbers_of_cores) as pool:
        pool.starmap(annotation_func, data)
    
    concat_df = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_1.p', 'rb'))
    if par_core > 1:
        for i in range(par_core):
            df_dynamic = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_{i+1}.p', 'rb'))
            concat_df = pd.concat([concat_df, df_dynamic])


    pickle.dump(concat_df, open(f'./output_ParallelPyMetaMap_{column_name}/annotated_df/annotated_{column_name}_{unique_id}_df.p', 'wb'))
    
    if update == True:
        final_df = pd.concat([df_processed, concat_df])
    else:
        final_df = concat_df

    pickle.dump(final_df, open(f'./output_ParallelPyMetaMap_{column_name}/annotated_df/annotated_{column_name}_{unique_id}_df2.p', 'wb'))

    list1 = np.unique(df[unique_id])
    list2 = np.unique(concat_df[unique_id])
    avoid = np.setdiff1d(list1,list2)

    for i in range(len(avoid)):
        f = open(f"./output_ParallelPyMetaMap_{column_name}/to_avoid/{unique_id}_to_avoid.txt", "a")
        f.write(str(avoid[i]) + str('\n'))
        f.close()

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Process complete'))
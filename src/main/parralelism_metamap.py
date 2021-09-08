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

def removeNonAscii(s):
    return "".join(filter(lambda x: ord(x)<128, s))

def concept2dict(concepts):
    d = defaultdict(list)
    for c in concepts:
        d[c.index].append(c._asdict())
    return d

def annotation_func(df, batch, batch_size, restrict_to_sources, verbose, mm):
    list_of_semtypes = []
    list_of_cuis = []
    list_of_preferred_names = []
    list_of_annotations = []
    idx = []
    occurence = []
    negation = []
    for j in range(len(df['content_text'])):
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print(str(current_time) + str(' We are starting processing row number ') + str(j+1+int((batch-1)*batch_size)))
        print(str(current_time) + str(' We are at row ') + str(j+1) + str(' out of ') + str(len(df)) + str(' from proccess ') + bold.BEGIN + str(batch) + bold.END)
        print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str(round((float(j)/float(len(df)))*100, 2)) + str('%') + bold.END)
        print(str(current_time) + str(' Processing ') + str(df.index.values[j]))
        if df['content_text'][j] != df['content_text'][j] or df['content_text'][j] == None or df['content_text'][j] == '' or df['content_text'][j][:4] == 'ABS:':
            pass
        else:
            term = removeNonAscii(df['content_text'][j])
            term = [term]
            ids = list(range(len(term)))
            concepts, error = mm.extract_concepts(term, 
                                                 ids = ids, 
                                                 restrict_to_sources = restrict_to_sources,
                                                 ignore_stop_phrases = True,
                                                 word_sense_disambiguation=True,
                                                 verbose=verbose,
                                                 no_derivational_variants=True)
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
                    idx.append(df.index.values[j])

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
                'master_df2_idx' : idx
                })
            pickle.dump(annotated_df, open(f'master_annotated_working_df2_{batch}.p', 'wb'))
                
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
        'master_df2_idx' : idx
        })
    pickle.dump(annotated_df, open(f'master_annotated_working_df2_{batch}.p', 'wb'))
                      

def parralelism_metamap(numbers_of_cores,
                        path_to_metamap,
                        path_to_file = None,
                        file = None,
                        restrict_to_sources= [],
                        verbose = False):

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
    
    pmids = []
    for i in range(len(df)):
        if df['content_text'][i] != df['content_text'][i] or df['content_text'][i] == None or df['content_text'][i] == '' or df['content_text'][i][:4] == 'ABS:':
            pass
        else:
            if len(df.iloc[i].content_text.split()) > 150000:
                pass
            else:
                pmids.append(df.iloc[i].pmid)
    df = df[df.pmid.isin(pmids)]

    update = False

    retrieved_path = [path for path in Path('./').iterdir() if path.stem == 'master_annotated_working_df2']
    if len(retrieved_path) == 0:
        update = False
    elif retrieved_path[0]:
        update = True
    else:
        update = False


    if update == True:

        df_processed = pickle.load(open('master_annotated_working_df2.p', 'rb'))

        list_original_index = []
        for i in range(len(df)):
            list_original_index.append(df.index[i])

        list_check_index = []
        list_check_index = list(np.unique(df_processed.master_df2_idx))

        list_to_do = list(set(list_original_index) - set(list_check_index))

        subset_list_update = []
        for i in range(len(list_to_do)):
            subset_list_update.append(df.loc[list_to_do[i]].pmid)

        df = df[df.pmid.isin(subset_list_update)]

    mm = MetaMap.get_instance(path_to_metamap)

    df_size = len(df['content_text'])
    batch_size = round(len(df)/par_core)

    if par_core > 1:
        data = []
        for i in range(par_core):
            if i == 0:
                current_data = (df[:round(len(df)/par_core)], i+1, batch_size, restrict_to_sources, verbose, mm)
                data.append(current_data)
            elif i > 0 and i+1 != par_core:
                current_data = (df[(round(len(df)/par_core))*i:(round(len(df)/par_core))*(i+1)], i+1, batch_size, restrict_to_sources, verbose, mm)
                data.append(current_data)
            else:
                current_data = (df[round(len(df)/par_core)*i:], i+1, batch_size, restrict_to_sources, verbose, mm)
                data.append(current_data)
    else:
        data = [(df, 1, batch_size, restrict_to_sources, verbose, mm)]

    with mp.Pool(numbers_of_cores) as pool:
        pool.starmap(annotation_func, data)
    
    concat_df = pickle.load(open(f'master_annotated_working_df2_1.p', 'rb'))
    if numbers_of_cores > 1:
        for i in range(par_core):
            df_dynamic = pickle.load(open(f'master_annotated_working_df2_{i+1}.p', 'rb'))
            concat_df = pd.concat([concat_df, df_dynamic])


    pickle.dump(concat_df, open(f'master_annotated_working_df.p', 'wb'))
    
    if update == True:
        final_df = pd.concat([df_processed, concat_df])
    else:
        final_df = concat_df

    pickle.dump(final_df, open(f'master_annotated_working_df2.p', 'wb'))


    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Process complete'))

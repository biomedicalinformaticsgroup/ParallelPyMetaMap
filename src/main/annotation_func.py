from datetime import datetime
import pickle
import pandas as pd 
from ParallelPyMetaMap.src.altered_pymetamap.MetaMap import MetaMap
from ParallelPyMetaMap.src.altered_pymetamap.SubprocessBackend import SubprocessBackend
from ParallelPyMetaMap.src.main.removeNonAscii import removeNonAscii
from ParallelPyMetaMap.src.main.concept2dict import concept2dict

class bold:
   BEGIN = '\033[1m'
   END = '\033[0m'

def annotation_func(df, 
                    batch, 
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
    occurrence = []
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
                    occurrence.append(neg + (int(str(text).count('noun-0')) + int(str(text).count('adj-0')) + int(str(text).count('verb-0')) + int(str(text).count('adv-0')) + int(str(text).count('integer-0')) + int(str(text).count('numeral-0')) + int(str(text).count('prep-0')) + int(str(text).count('number-0')) + int(str(text).count('percentage-0')) + int(str(text).count('conj-0')) + int(str(text).count('ordinal-0')) + int(str(text).count('UNKNOWN-0')) + int(str(text).count('aux-0')) + int(str(text).count('fraction-0'))))
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
        
        if df.iloc[j][unique_id] not in idx:
            f = open(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/{unique_id}_to_avoid.txt", "a")
            f.write(str(df.iloc[j][unique_id]) + str('\n'))
            f.close()

        if (j % 100 == 0 and j != 0):
            now = datetime.now()
            current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
            print(str(current_time) + str(' Saving file'))
            annotated_df = pd.DataFrame(
                {'semantic_type': list_of_semtypes,
                'umls_preferred_name': list_of_preferred_names,
                'occurrence': occurrence,
                'negation': negation,
                'cui': list_of_cuis,
                'annotation': list_of_annotations,
                f'{unique_id}' : idx
                })
            pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
                
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str(round((float(j+1)/float(len(df)))*100, 2)) + str('%') + bold.END)
    annotated_df = pd.DataFrame(
        {'semantic_type': list_of_semtypes,
        'umls_preferred_name': list_of_preferred_names,
        'occurrence': occurrence,
        'negation': negation,
        'cui': list_of_cuis,
        'annotation': list_of_annotations,
        f'{unique_id}' : idx
        })

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Adding full_semantic_type_name and semantic_group_name to the result for ')+ str('proccess ') + bold.BEGIN + str(batch) + bold.END)
    
    annotated_df['semantic_type'] = annotated_df['semantic_type'].str.strip('[]').str.split(',')

    df_semantictypes = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/extra_resources/df_semantictypes.p', 'rb'))
    df_semgroups = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}/extra_resources/df_semgroups.p', 'rb'))

    full_semantic_type_name_list = []
    for i in range(len(annotated_df)):
        full_semantic_type_name_list_current = []
        for j in range(len(annotated_df.iloc[i].semantic_type)): 
            full_semantic_type_name_list_current.append(df_semantictypes[df_semantictypes.abbreviation == annotated_df.iloc[i].semantic_type[j]].full_semantic_type_name.values[0])
        full_semantic_type_name_list.append(full_semantic_type_name_list_current)
    annotated_df["full_semantic_type_name"] = full_semantic_type_name_list

    semantic_group_name_list = []
    for i in range(len(annotated_df)):
        semantic_group_name_list_current = []
        for j in range(len(annotated_df.iloc[i].semantic_type)): 
            semantic_group_name_list_current.append(df_semgroups[df_semgroups.full_semantic_type_name == annotated_df.iloc[i].full_semantic_type_name[j]].semantic_group_name.values[0])
        semantic_group_name_list.append(semantic_group_name_list_current)
    annotated_df["semantic_group_name"] = semantic_group_name_list    

    annotated_df = annotated_df[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'annotation', f'{unique_id}']]

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Saving file for proccess ') + bold.BEGIN + str(batch) + bold.END)
    pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
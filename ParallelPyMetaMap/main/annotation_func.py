from datetime import datetime
import pickle
import pandas as pd 
import numpy as np
import re
from ParallelPyMetaMap.altered_pymetamap.MetaMap import MetaMap
from ParallelPyMetaMap.altered_pymetamap.SubprocessBackend import SubprocessBackend
from ParallelPyMetaMap.main.removeNonAscii import removeNonAscii
from ParallelPyMetaMap.main.concept2dict import concept2dict

class bold:
   BEGIN = '\033[1m'
   END = '\033[0m'

def annotation_func(df, 
                    batch, 
                    mm,
                    column_name,
                    out_form,
                    unique_id,
                    extension,
                    extension_format,
                    composite_phrase,
                    fielded_mmi_output,
                    machine_output,
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
                    mm_data_year,
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
    score = []
    cui = []
    prefered_name = []
    trigger = []
    semantic_list = []
    sab = []
    pos_info = []
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
                                                 fielded_mmi_output,
                                                 machine_output,
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
                                                 mm_data_year,
                                                 verbose,
                                                 exclude_sources,
                                                 restrict_to_sources,
                                                 restrict_to_sts,
                                                 exclude_sts,
                                                 no_nums)
            if fielded_mmi_output == True:
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
                        occurrence.append(str(np.unique(str(data[str(0)][i].get('pos_info').replace(';', ',')).split(','))).count('/'))
                        idx.append(df.iloc[j][unique_id])
                        if extension_format == 'dict':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            f.write(str(str(data[str(0)][i]) + ('\n')))
                            f.close()
                        elif extension_format == 'terminal':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            f.write(str('USER|') + str(data[str(0)][i].get('mm')) + str('|') + str(data[str(0)][i].get('score')) + str('|') + str(data[str(0)][i].get('preferred_name')) + str('|') + str(data[str(0)][i].get('cui')) + str('|') + str(data[str(0)][i].get('semtypes')) + str('|') + str(data[str(0)][i].get('trigger')) + str('|') + str(data[str(0)][i].get('location')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('|') + str(data[str(0)][i].get('tree_codes')) + str('\n')) 
                            f.close()

                    if 'aa' in data[str(0)][i]:
                        if extension_format == 'dict':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            f.write(str(str(data[str(0)][i]) + ('\n')))
                            f.close()
                        elif extension_format == 'terminal':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            f.write(str('USER|') + str(data[str(0)][i].get('aa')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('\n')) 
                            f.close()

                    if 'ua' in data[str(0)][i]:
                        if extension_format == 'dict':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            f.write(str(str(data[str(0)][i]) + ('\n')))
                            f.close()
                        elif extension_format == 'terminal':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            f.write(str('USER|') + str(data[str(0)][i].get('ua')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('\n')) 
                            f.close()

            if machine_output == True:
                temp_list = []
                for i in range(len(concepts)):
                    if 'ev(-' in concepts[i]:
                        temp_list.append(concepts[i])
                temp_anno = []
                for i in range(len(temp_list)):
                    temp_list_current = temp_list[i].split('ev(-')
                    temp_list_current_correct = []
                    for k in range(1,len(temp_list_current)):
                        temp_list_current_correct.append(str('ev(-') + str(temp_list_current[k]))
                    for k in range(len(temp_list_current_correct)):
                        matches = re.findall('ev\(-.+?,0\)',temp_list_current_correct[k])
                        for g in range(len(matches)):
                            temp_anno.append(matches[g])
                        matches = re.findall('ev\(-.+?,1\)',temp_list_current_correct[k])
                        for g in range(len(matches)):
                            temp_anno.append(matches[g])
                for i in range(len(temp_anno)):
                    current_square_count = re.findall('\[.+?\],',temp_anno[i])
                    current_pos_count = current_square_count[-1][:-1]
                    count_value = str(current_pos_count).count('/')
                    for u in range(count_value):
                        current_score = re.findall('ev\(-.+?,',temp_anno[i])
                        if temp_anno[i][-2] == '1':
                            current_score = str('-') + str(current_score[0][4:-1])
                        if temp_anno[i][-2] == '0':
                            current_score = current_score[0][4:-1]
                        curent_cui = str(temp_anno[i].split(',')[1]).strip("'")
                        current_prefered = str(temp_anno[i].split(',')[3]).strip("'")
                        current_square = re.findall('\[.+?\],',temp_anno[i])
                        current_semantic = current_square[1][:-1]
                        current_sab = current_square[-2][:-1]
                        current_pos = current_square[-1].strip('[]').split(',')[u]
                        current_trigger = df[column_name][j][int(current_pos.strip('[]').split('/')[0])-1:int(int(current_pos.strip('[]').split('/')[0]) + int(current_pos.strip('[]').split('/')[1]))-1]
                        current_occurrence = 1
                        if temp_anno[i][-2] == '1':
                            current_negation = 1
                        if temp_anno[i][-2] == '0':
                            current_negation = 0
                        score.append(int(current_score))
                        cui.append(str(curent_cui))
                        prefered_name.append(str(current_prefered))
                        trigger.append(str(current_trigger))
                        semantic_list.append(current_semantic.strip('[]').replace("\'", '').split(','))
                        sab.append(current_sab.strip('[]').replace("\'", '').split(','))
                        pos_info.append(current_pos)
                        occurrence.append(int(current_occurrence))
                        negation.append(int(current_negation))
                        idx.append(str(df.iloc[j][unique_id]))
                if extension_format == 'dict':
                    for i in range(len(temp_anno)):
                        f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str(str(temp_anno[i]) + ('\n')))
                        f.close()
                elif extension_format == 'terminal':
                    for i in range(len(temp_anno)):
                        f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                        f.write(str(str(temp_anno[i]) + ('\n')))
                        f.close()
            
        if df.iloc[j][unique_id] not in idx:
            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/{unique_id}_to_avoid.txt", "a")
            f.write(str(df.iloc[j][unique_id]) + str('\n'))
            f.close()

        if fielded_mmi_output == True:
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
                pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
        if machine_output == True:
            if (j % 100 == 0 and j != 0):
                now = datetime.now()
                current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
                print(str(current_time) + str(' Saving file'))
                annotated_df = pd.DataFrame(
                    {'score': score, 
                    'cui': cui, 
                    'prefered_name': prefered_name, 
                    'trigger': trigger, 
                    'semantic_type': semantic_list, 
                    'sab': sab, 
                    'pos_info': pos_info, 
                    'occurrence': occurrence, 
                    'negation': negation, 
                    f'{unique_id}': idx
                    })
                pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
                
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str(round((float(j+1)/float(len(df)))*100, 2)) + str('%') + bold.END)
    if fielded_mmi_output == True:
        annotated_df = pd.DataFrame(
            {'semantic_type': list_of_semtypes,
            'umls_preferred_name': list_of_preferred_names,
            'occurrence': occurrence,
            'negation': negation,
            'cui': list_of_cuis,
            'annotation': list_of_annotations,
            f'{unique_id}' : idx
            })
    if machine_output == True:
        annotated_df = pd.DataFrame(
            {'score': score, 
            'cui': cui, 
            'prefered_name': prefered_name, 
            'trigger': trigger, 
            'semantic_type': semantic_list, 
            'sab': sab, 
            'pos_info': pos_info, 
            'occurrence': occurrence, 
            'negation': negation, 
            f'{unique_id}': idx
            })
        annotated_df = annotated_df.drop_duplicates(subset=['cui', 'trigger', 'pos_info', f'{unique_id}'])
        annotated_df = annotated_df.reset_index(drop=True)
        annotated_df['pos_info'] = annotated_df['pos_info'].str.strip('[]').str.split(',')
        aggregation_functions = {'occurrence': 'sum', 'negation': 'sum', 'sab': lambda x: list(x), 'trigger': lambda x: list(x), 'score': lambda x: list(x), 'pos_info': lambda x: list(x), 'prefered_name': 'first', 'semantic_type': 'first'}
        annotated_df = annotated_df.groupby(['cui', f'{unique_id}']).aggregate(aggregation_functions)
        annotated_df = annotated_df.reset_index()

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Adding full_semantic_type_name and semantic_group_name to the result for ')+ str('proccess ') + bold.BEGIN + str(batch) + bold.END)
    
    if fielded_mmi_output == True:
        annotated_df['semantic_type'] = annotated_df['semantic_type'].str.strip('[]').str.split(',')

    df_semantictypes = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.p', 'rb'))
    df_semgroups = pickle.load(open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.p', 'rb'))

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

    if fielded_mmi_output == True:
        annotated_df = annotated_df[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'annotation', f'{unique_id}']]
    if machine_output == True:
        annotated_df = annotated_df[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print(str(current_time) + str(' Saving file for proccess ') + bold.BEGIN + str(batch) + bold.END)
    pickle.dump(annotated_df, open(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df/annotated_{column_name}_df2_{batch}.p', 'wb'))
from datetime import datetime
import json
import pandas as pd 
import numpy as np
import re
import unicodedata
import json
from ParallelPyMetaMap.altered_pymetamap.MetaMap import MetaMap
from ParallelPyMetaMap.altered_pymetamap.SubprocessBackend import SubprocessBackend
from ParallelPyMetaMap.main.removeNonAscii import removeNonAscii
from ParallelPyMetaMap.main.concept2dict import concept2dict
import zipfile
import os

class bold:
   BEGIN = '\033[1m'
   END = '\033[0m'

def annotation_func(df, 
                    batch, 
                    mm,
                    cadmus,
                    path_to_directory,
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
    with zipfile.ZipFile(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.json.zip', "r") as z:
        for sem_filename in z.namelist():  
            with z.open(sem_filename) as f:  
                d = f.read()  
                d = json.loads(d)    
            f.close()
    z.close()
    df_semantictypes = pd.read_json(d, orient='index')
    semantictypes_abbr = list(df_semantictypes.abbreviation)
    semantictypes_full_semantic_type_name = list(df_semantictypes.full_semantic_type_name)
    semantictypes_dict = {semantictypes_abbr[i]: semantictypes_full_semantic_type_name[i] for i in range(len(semantictypes_abbr))}
    with zipfile.ZipFile(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semgroups.json.zip', "r") as z:
        for group_filename in z.namelist():  
            with z.open(group_filename) as f:  
                d = f.read()  
                d = json.loads(d)    
            f.close()
    z.close()
    df_semgroups = pd.read_json(d, orient='index')
    semanticgroup_sem_name = list(df_semgroups.full_semantic_type_name)
    semanticgroup_sema_group_name = list(df_semgroups.semantic_group_name)
    semanticgroup_dict = {semanticgroup_sem_name[i]: semanticgroup_sema_group_name[i] for i in range(len(semanticgroup_sem_name))}
    for j in range(len(df)):
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
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print(str(current_time) + str(' We are at row ') + str(j+1) + str(' out of ') + str(len(df)) + str(' from proccess ') + bold.BEGIN + str(batch) + bold.END)
        print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str(round((float(j)/float(len(df)))*100, 2)) + str('%') + bold.END)
        print(str(current_time) + str(' Processing ') + str(df.iloc[j][unique_id]))
        if df[column_name].iloc[j] != df[column_name].iloc[j] or df[column_name].iloc[j] == None or df[column_name].iloc[j] == '':
            pass
        else:
            if cadmus == None and path_to_directory == None:
                term = removeNonAscii(df[column_name].iloc[j])
            else:
                if '.zip' in df['file_path'].iloc[j]:
                    with zipfile.ZipFile(f"{df['file_path'].iloc[j]}", "r") as z:
                        for path_filename in z.namelist():
                            with z.open(path_filename) as f:
                                term = f.readline()
                            f.close()
                    z.close()
                    if type(term) is str:
                        pass
                    elif type(term) is bytes:
                        term = term.decode('utf-8')
                    term = term.replace('\n', '')
                    term = removeNonAscii(term)
                else:
                    f = open(f"{df['file_path'].iloc[j]}", "r")
                    term = f.readline()
                    f.close()
                    if type(term) is str:
                        pass
                    elif type(term) is bytes:
                        term = term.decode('utf-8')
                    term = term.replace('\n', '')
                    term = removeNonAscii(term)
            term = unicodedata.normalize("NFKD", term)
            term = term.replace('\n', ' ')
            term = term.replace('\r', ' ')
            term = term.replace('\t', ' ')
            term = term.replace('\'', ' ')
            term = term.replace('\"', ' ')
            term = term.replace('\\', ' ')
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

                for i in range(len(data[str(0)])):
                    if 'preferred_name' and 'cui' in data[str(0)][i]:
                        if 'semtypes' in data[str(0)][i]:
                            list_of_semtypes.append(data[str(0)][i]['semtypes'])
                        else:
                            list_of_semtypes.append(None)
                        list_of_preferred_names.append(data[str(0)][i]['preferred_name'])
                        list_of_cuis.append(data[str(0)][i]['cui'])
                        list_of_annotations.append(data[str(0)][i])
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
                            if i != int((len(data[str(0)])-1)): 
                                f.write(str(str(data[str(0)][i]) + ('\n')))
                            else:
                                f.write(str(data[str(0)][i]))
                            f.close()
                        elif extension_format == 'terminal':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            if i != int((len(data[str(0)])-1)):
                                f.write(str('USER|') + str(data[str(0)][i].get('aa')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('\n')) 
                            else:
                                f.write(str('USER|') + str(data[str(0)][i].get('aa')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info'))) 
                            f.close()

                    if 'ua' in data[str(0)][i]:
                        if extension_format == 'dict':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            if i != int((len(data[str(0)])-1)):
                                f.write(str(str(data[str(0)][i]) + ('\n')))
                            else:
                                f.write(str(data[str(0)][i]))
                            f.close()
                        elif extension_format == 'terminal':
                            f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", "a")
                            if i != int((len(data[str(0)])-1)):
                                f.write(str('USER|') + str(data[str(0)][i].get('ua')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info')) + str('\n')) 
                            else:
                                f.write(str('USER|') + str(data[str(0)][i].get('ua')) + str('|') + str(data[str(0)][i].get('short_form')) + str('|') + str(data[str(0)][i].get('long_form')) + str('|') + str(data[str(0)][i].get('num_tokens_short_form')) + str('|') + str(data[str(0)][i].get('num_chars_short_form')) + str('|') + str(data[str(0)][i].get('num_tokens_long_form')) + str('|') + str(data[str(0)][i].get('num_chars_long_form')) + str('|') + str(data[str(0)][i].get('pos_info'))) 
                            f.close()

            if machine_output == True:

                temp_list_ut = []
                for i in range(len(concepts)):
                    if "phrase(" in concepts[i]:
                        temp_list_ut.append(concepts[i])
                text_list = []
                for i in range(len(temp_list_ut)):
                    matches = re.findall('phrase\(.+?,\[',temp_list_ut[i])
                    size_text = int(str((temp_list_ut[i].split(',')[-2])).split('/')[1])
                    matches[0] = matches[0].replace("\'", "")
                    matches[0] = matches[0].replace('\"' , '')
                    if len(matches[0][7:-2]) != size_text:
                        diff = len(matches[0][7:-2]) - size_text
                        text = (matches[0][int(7+(diff/2)):-int(2+diff/2)], int(str((temp_list_ut[i].split(',')[-2])).split('/')[0]))
                        text_list.append(text)
                    else:
                        text = (matches[0][7:-2], int(str((temp_list_ut[i].split(',')[-2])).split('/')[0]))
                        text_list.append(text)
                full_text = str
                for i in range(len(text_list)):
                    if i == 0:
                        full_text = text_list[i][0]
                    else:
                        diff = text_list[i][1] - text_list[i-1][1] - len(text_list[i-1][0])
                        if diff != 0:
                            if diff < 0:
                                full_text = full_text[:diff]
                            else:
                                for k in range(diff):
                                    full_text = str(str(full_text) + str(' '))
                            full_text = full_text + text_list[i][0]
                        else:
                            full_text = full_text + text_list[i][0]

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
                        current_prefered_search = re.findall(',\'.+?,\[',temp_anno[i])
                        current_prefered = ','.join(current_prefered_search[0].split(",")[3:-1]).replace("'", "")
                        current_square = re.findall('\[.+?\],',temp_anno[i])
                        current_semantic_identification = re.findall(',\[.+[a-z,]\],', temp_anno[i])
                        current_semantic = current_semantic_identification[0].split(',[')[-1][:-2]
                        current_sab = current_square[-2][:-1]
                        current_pos = current_square[-1].strip('[]').split(',')[u]
                        current_trigger = full_text[int(current_pos.strip('[]').split('/')[0]):int(int(current_pos.strip('[]').split('/')[0]) + int(current_pos.strip('[]').split('/')[1]))]
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

                for i in range(len(temp_anno)):
                    f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output/{df.iloc[j][unique_id]}.{extension}", "a")
                    if i != int(len(temp_anno)-1):
                        f.write(str(str(temp_anno[i]) + ('\n')))
                    else:
                        f.write(str(temp_anno[i]))
                    f.close()
                
                f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input/{df.iloc[j][unique_id]}.{extension}", "a")
                f.write(str(full_text))
                f.close()
      
            if df.iloc[j][unique_id] not in idx:
                f = open(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/{unique_id}_to_avoid.txt", "a")
                f.write(str(df.iloc[j][unique_id]) + str('\n'))
                f.close()
            else:
                if fielded_mmi_output == True:
                    zipfile.ZipFile(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}.zip', mode='w').write(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}", arcname=f'{df.iloc[j][unique_id]}.{extension}')
                    os.remove(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/{df.iloc[j][unique_id]}.{extension}')
                if machine_output == True:
                    zipfile.ZipFile(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input/{df.iloc[j][unique_id]}.{extension}.zip', mode='w').write(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input/{df.iloc[j][unique_id]}.{extension}", arcname=f'{df.iloc[j][unique_id]}.{extension}')
                    os.remove(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input/{df.iloc[j][unique_id]}.{extension}')
                    zipfile.ZipFile(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output/{df.iloc[j][unique_id]}.{extension}.zip', mode='w').write(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output/{df.iloc[j][unique_id]}.{extension}', arcname=f'{df.iloc[j][unique_id]}.{extension}')
                    os.remove(f'./output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output/{df.iloc[j][unique_id]}.{extension}')
                if fielded_mmi_output == True:
                    annotated_df = pd.DataFrame(
                        {'semantic_type': list_of_semtypes,
                        'umls_preferred_name': list_of_preferred_names,
                        'occurrence': occurrence,
                        'cui': list_of_cuis,
                        'annotation': list_of_annotations,
                        f'{unique_id}' : idx
                        })
                    annotated_df['semantic_type'] = annotated_df['semantic_type'].str.strip('[]').str.split(',')

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
                    aggregation_functions = {'occurrence': 'sum', 'negation': 'sum', 'sab': 'first', 'trigger': lambda x: list(x), 'score': lambda x: list(x), 'pos_info': lambda x: list(x), 'prefered_name': 'first', 'semantic_type': 'first'}
                    annotated_df = annotated_df.groupby(['cui', f'{unique_id}']).aggregate(aggregation_functions)
                    annotated_df = annotated_df.reset_index()

                full_semantic_type_name_list = []
                for i in range(len(annotated_df)):
                    full_semantic_type_name_list_current = []
                    for j in range(len(annotated_df.iloc[i].semantic_type)): 
                        full_semantic_type_name_list_current.append(semantictypes_dict.get(annotated_df.iloc[i].semantic_type[j]))
                    full_semantic_type_name_list.append(full_semantic_type_name_list_current)
                annotated_df["full_semantic_type_name"] = full_semantic_type_name_list

                semantic_group_name_list = []
                for i in range(len(annotated_df)):
                    semantic_group_name_list_current = []
                    for j in range(len(annotated_df.iloc[i].semantic_type)): 
                        semantic_group_name_list_current.append(semanticgroup_dict.get(annotated_df.iloc[i].full_semantic_type_name[j]))
                    semantic_group_name_list.append(semantic_group_name_list_current)
                annotated_df["semantic_group_name"] = semantic_group_name_list  

                if fielded_mmi_output == True:
                    annotated_df = annotated_df[['cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation', f'{unique_id}']]
                    current_dict = {}
                    for i in range(len(annotated_df)):
                        current_dict[f"{annotated_df.iloc[i].cui}"] = {
                            'umls_preferred_name': annotated_df.iloc[i].umls_preferred_name,
                            'semantic_type': annotated_df.iloc[i].semantic_type,
                            'full_semantic_type_name': annotated_df.iloc[i].full_semantic_type_name,
                            'semantic_group_name': annotated_df.iloc[i].semantic_group_name,
                            'occurrence': float(annotated_df.iloc[i].occurrence),
                            'annotation': annotated_df.iloc[i].annotation
                        }
                    with zipfile.ZipFile(f"output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json/{idx[0]}.json.zip", mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                        dumped_JSON: str = json.dumps(current_dict, indent=4)
                        zip_file.writestr(f"{idx[0]}.json", data=dumped_JSON)
                        zip_file.testzip()
                    zip_file.close()
                if machine_output == True:
                    annotated_df = annotated_df[['cui', 'prefered_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'negation', 'trigger', 'sab', 'pos_info', 'score', f'{unique_id}']]
                    current_dict = {}
                    for i in range(len(annotated_df)):
                        current_dict[f"{annotated_df.iloc[i].cui}"] = {
                            'prefered_name': annotated_df.iloc[i].prefered_name,
                            'semantic_type': annotated_df.iloc[i].semantic_type,
                            'full_semantic_type_name': annotated_df.iloc[i].full_semantic_type_name,
                            'semantic_group_name': annotated_df.iloc[i].semantic_group_name,
                            'occurrence': float(annotated_df.iloc[i].occurrence),
                            'negation': float(annotated_df.iloc[i].negation),
                            'trigger': annotated_df.iloc[i].trigger,
                            'sab': annotated_df.iloc[i].sab,
                            'pos_info': annotated_df.iloc[i].pos_info,
                            'score': annotated_df.iloc[i].score
                        }
                    with zipfile.ZipFile(f"output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json/{idx[0]}.json.zip", mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                        dumped_JSON: str = json.dumps(current_dict, indent=4)
                        zip_file.writestr(f"{idx[0]}.json", data=dumped_JSON)
                        zip_file.testzip()
                    zip_file.close()
    print(str('Proccess ') + bold.BEGIN + str(batch) + bold.END + str(' has completed ') + bold.BEGIN + str('100%') + bold.END)
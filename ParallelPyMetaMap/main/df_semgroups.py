import requests
import pickle
import pandas as pd

def df_semgroups(column_name):
    url = "https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt"
    file = requests.get(url)

    semgroups_sentences = str(file.text).split('\n')[:-1]

    file.close()

    semantic_group_abbrev = []
    semantic_group_name = []
    type_unique_identifier = []
    full_semantic_type_name = []

    for i in range(len(semgroups_sentences)):
        semantic_group_abbrev.append(semgroups_sentences[i].split('|')[0])
        semantic_group_name.append(semgroups_sentences[i].split('|')[1])
        type_unique_identifier.append(semgroups_sentences[i].split('|')[2])
        full_semantic_type_name.append(semgroups_sentences[i].split('|')[3])

    dict = {'semantic_group_abbrev': semantic_group_abbrev, 'semantic_group_name' : semantic_group_name, 'type_unique_identifier': type_unique_identifier, 'full_semantic_type_name': full_semantic_type_name} 
    df_semgroups = pd.DataFrame(dict)
    pickle.dump(df_semgroups, open(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/df_semgroups.p", "wb"))
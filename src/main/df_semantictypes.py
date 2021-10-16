import requests
import pickle
import pandas as pd

def df_semantictypes(column_name):
    url = "https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt"
    file = requests.get(url)

    semantictypes_sentences = str(file.text).split('\n')[:-1]

    file.close()

    abbreviation = []
    type_unique_identifier = []
    full_semantic_type_name = []

    for i in range(len(semantictypes_sentences)):
        abbreviation.append(semantictypes_sentences[i].split('|')[0])
        type_unique_identifier.append(semantictypes_sentences[i].split('|')[1])
        full_semantic_type_name.append(semantictypes_sentences[i].split('|')[2])

    dict = {'abbreviation': abbreviation, 'type_unique_identifier': type_unique_identifier, 'full_semantic_type_name': full_semantic_type_name} 
    df_semantictypes = pd.DataFrame(dict)
    pickle.dump(df_semantictypes, open(f"./output_ParallelPyMetaMap_{column_name}/extra_resources/df_semantictypes.p", "wb"))
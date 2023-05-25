import requests
import pickle
import json
import pandas as pd
import zipfile

def df_semantictypes(column_name, out_form):
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
    result = df_semantictypes.to_json(orient="index")
    with zipfile.ZipFile(f"./output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/df_semantictypes.json.zip", mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
        dumped_JSON: str = json.dumps(result, indent=4)
        zip_file.writestr("df_semantictypes.json", data=dumped_JSON)
        zip_file.testzip()
    zip_file.close()
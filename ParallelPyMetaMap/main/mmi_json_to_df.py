import pickle
import pandas as pd
import numpy as np
import glob
import json
import zipfile

class FileReaderAll:
    def __init__(self, file_path):
        if file_path[-5:] == '.json':
            with open(file_path) as file:
                content = json.load(file)
            file.close()
        else:
            with zipfile.ZipFile(file_path, "r") as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        d = f.read()
                        content = json.loads(d)
                    f.close()
            z.close()
        file_name = filename.split('/')[-1].split('.')[0]
        self.id = []
        self.cui = []
        self.umls_preferred_name = []
        self.semantic_type = []
        self.full_semantic_type_name = []
        self.semantic_group_name = []
        self.occurrence = []
        self.annotation = []
        for key in content:
            self.id.append(file_name)
            self.cui.append(key)
            self.umls_preferred_name.append(content[key].get('umls_preferred_name'))
            self.semantic_type.append(content[key].get('semantic_type'))
            self.full_semantic_type_name.append(content[key].get('full_semantic_type_name'))
            self.semantic_group_name.append(content[key].get('semantic_group_name'))
            self.occurrence.append(content[key].get('occurrence'))
            self.annotation.append(content[key].get('annotation'))
                
class FileReaderFiltering:
    def __init__(self, file_path, filtering):
        if file_path[-5:] == '.json':
            with open(file_path) as file:
                content = json.load(file)
            file.close()
        else:
            with zipfile.ZipFile(file_path, "r") as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        d = f.read()
                        content = json.loads(d)
                    f.close()
            z.close()
        file_name = file.name.split('/')[-1].split('.')[0]
        self.id = []
        self.cui = []
        self.umls_preferred_name = []
        self.semantic_type = []
        self.full_semantic_type_name = []
        self.semantic_group_name = []
        self.occurrence = []
        self.annotation = []
        for key in content:
            self.id.append(file_name)
            if 'cui' in filtering:
                self.cui.append(key)
            if 'umls_preferred_name' in filtering:
                self.prefered_name.append(content[key].get('umls_preferred_name'))
            if 'semantic_type' in filtering:
                self.semantic_type.append(content[key].get('semantic_type'))
            if 'full_semantic_type_name' in filtering:
                self.full_semantic_type_name.append(content[key].get('full_semantic_type_name'))
            if 'semantic_group_name' in filtering:
                self.semantic_group_name.append(content[key].get('semantic_group_name'))
            if 'occurrence' in filtering:
                self.occurrence.append(content[key].get('occurrence'))
            if 'annotation' in filtering:
                self.negation.append(content[key].get('annotation'))

def mmi_json_to_df(path = './', filtering = [], previous_df = None):
    if len(filtering) == 0:
        all_variables = True
    else:
        all_variables = False
    root_path = path
    all_zip = glob.glob(f'{root_path}/*.json.zip', recursive=True)
    all_json = glob.glob(f'{root_path}/*.json', recursive=True)
    all_files = all_zip + all_json    
    if type(previous_df) == pd.core.frame.DataFrame or type(previous_df) == str:
        if type(previous_df) == str and previous_df[-5:] == '.json':
            f = open(previous_df)
            data = json.load(f)
            f.close()
            previous_df = pd.read_json(data, orient='index')
        elif type(previous_df) == str and previous_df[-4:] == '.zip':
            with zipfile.ZipFile(previous_df, "r") as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        d = f.read()
                        d = json.loads(d)
                    f.close()
            z.close()
            previous_df = pd.read_json(d, orient='index')
        elif type(previous_df) == str and previous_df[-2:] == '.p':
            previous_df = pickle.load(open(previous_df, 'rb'))
        elif type(previous_df) == pd.core.frame.DataFrame:
            pass
        else:
            return 'previous_df parameters formats accepted are: Pandas DataFrame, path to .json, path to .json.zip, path to pickle object .p'
        list_done = list(np.unique(previous_df.id))
        list_done_json = [s + '.json' for s in list_done]
        list_done_zip = [s + '.json.zip' for s in list_done]
        if path[-1] == '/':
            list_done_json = [path + s for s in list_done_json]
            list_done_zip = [path + s for s in list_done_zip]
        else:
            list_done_json = [path + '/' + s for s in list_done_json]
            list_done_zip = [path + '/' + s for s in list_done_zip]
        all_files = list(set(all_files) - set(list_done_json))
        all_files = list(set(all_files) - set(list_done_zip))
    else:
        if previous_df == None:
            pass
        else:
            return 'Input type for the parameter previous_df is invalid, path string to df or df are acceptable input formats.'
    if all_variables == True:
        dict_ = {'id': [], 'cui': [], 'umls_preferred_name': [], 'semantic_type': [], 'full_semantic_type_name': [], 'semantic_group_name': [], 'occurrence': [], 'annotation': []}
        for idx, entry in enumerate(all_files):
            if len(all_files) > 100:
                if idx % (len(all_files) // 10) == 0:
                    print(f'Processing index: {idx} of {len(all_files)}')
            content = FileReaderAll(entry)
            dict_['id'].extend(content.id)
            dict_['cui'].extend(content.cui)
            dict_['umls_preferred_name'].extend(content.umls_preferred_name)
            dict_['semantic_type'].extend(content.semantic_type)
            dict_['full_semantic_type_name'].extend(content.full_semantic_type_name)
            dict_['semantic_group_name'].extend(content.semantic_group_name)
            dict_['occurrence'].extend(content.occurrence)
            dict_['annotation'].extend(content.annotation)
        df = pd.DataFrame(dict_, columns=['id', 'cui', 'umls_preferred_name', 'semantic_type', 'full_semantic_type_name', 'semantic_group_name', 'occurrence', 'annotation'])     
        df = pd.concat([df, previous_df], axis=0, join='outer', ignore_index=False, copy=True)
        df = df.reset_index(drop=True)
    else:
        dict_ = {'id': [], 'cui': [], 'umls_preferred_name': [], 'semantic_type': [], 'full_semantic_type_name': [], 'semantic_group_name': [], 'occurrence': [], 'annotation': []}
        for idx, entry in enumerate(all_files):
            if len(all_files) > 100:
                if idx % (len(all_files) // 10) == 0:
                    print(f'Processing index: {idx} of {len(all_files)}')
            content = FileReaderAll(entry)
            dict_['id'].extend(content.id)
            dict_['cui'].extend(content.cui)
            dict_['umls_preferred_name'].extend(content.umls_preferred_name)
            dict_['semantic_type'].extend(content.semantic_type)
            dict_['full_semantic_type_name'].extend(content.full_semantic_type_name)
            dict_['semantic_group_name'].extend(content.semantic_group_name)
            dict_['occurrence'].extend(content.occurrence)
            dict_['annotation'].extend(content.annotation)
        result = [dict_[key] for key in filtering]
        df = pd.DataFrame(result)
        print('Preparing the result, it might take a while')
        df = df.transpose()
        df.columns = filtering
        df = pd.concat([df, previous_df], axis=0, join='outer', ignore_index=False, copy=True)
        df = df.reset_index(drop=True)            
    return df
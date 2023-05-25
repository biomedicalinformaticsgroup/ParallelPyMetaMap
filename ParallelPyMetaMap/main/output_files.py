import os
import zipfile
import glob
import json
import pickle

def output_files(column_name, out_form, extension):
    #creating the directories we are planning on using the save the result of the system
    if out_form == 'mmi':
        for path in [f'output_ParallelPyMetaMap_{column_name}_{out_form}',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files'
                    ]:
            try:
                #try to create the directory, most likely will work for new project
                os.mkdir(path)
                print(f'Now creating {path}')
            except:
                #if the directory already exist just pass
                pass

    if out_form == 'mo':
        for path in [f'output_ParallelPyMetaMap_{column_name}_{out_form}',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input'
                    ]:
            try:
                #try to create the directory, most likely will work for new project
                os.mkdir(path)
                print(f'Now creating {path}')
            except:
                #if the directory already exist just pass
                pass

    if len(glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json/*.json', recursive=True)) > 0:
        print('Converting the json files from the annotated_json directory to zip files')
        all_ps = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_json/*.json', recursive=True)
        for i in range(len(all_ps)):
            zipfile.ZipFile(f'{all_ps[i]}.zip', mode='w').write(f'{all_ps[i]}', arcname=f'{all_ps[i].split("/")[-1].split(".")[0]}.json')
            os.remove(f'{all_ps[i]}')
    
    if len(glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/*.p', recursive=True)) > 0:
        print('Converting the pickle files from the extra_resources directory to json files')
        all_ps = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/*.p', recursive=True)
        for i in range(len(all_ps)):
            temp_p = pickle.load(open(f'{all_ps[i]}', 'rb'))
            result = temp_p.to_json(orient="index")
            json_object = json.dumps(result, indent=4)
            with open(f"output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/{all_ps[i].split('/')[-1].split('.')[0]}.json", "w") as outfile:
                outfile.write(json_object)
            outfile.close()
            os.remove(f'{all_ps[i]}')
    
    if len(glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/*.json', recursive=True)) > 0:
        print('Converting the json files from the extra_resources directory to zip files')
        all_ps = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/extra_resources/*.json', recursive=True)
        for i in range(len(all_ps)):
            zipfile.ZipFile(f'{all_ps[i]}.zip', mode='w').write(f'{all_ps[i]}', arcname=f'{all_ps[i].split("/")[-1].split(".")[0]}.json')
            os.remove(f'{all_ps[i]}')

    if out_form == 'mo':
        if len(glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output/*.{extension}', recursive=True)) > 0:
            print(f'Converting the {extension} files from the {extension}_files_output directory to zip files')
            all_ps = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_output/*.{extension}', recursive=True)
            for i in range(len(all_ps)):
                zipfile.ZipFile(f'{all_ps[i]}.zip', mode='w').write(f'{all_ps[i]}', arcname=f'{all_ps[i].split("/")[-1].split(".")[0]}.{all_ps[i].split("/")[-1].split(".")[-1]}')
                os.remove(f'{all_ps[i]}')
        
        if len(glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input/*.{extension}', recursive=True)) > 0:
            print(f'Converting the {extension} files from the {extension}_files_input directory to zip files')
            all_ps = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files_input/*.{extension}', recursive=True)
            for i in range(len(all_ps)):
                zipfile.ZipFile(f'{all_ps[i]}.zip', mode='w').write(f'{all_ps[i]}', arcname=f'{all_ps[i].split("/")[-1].split(".")[0]}.{all_ps[i].split("/")[-1].split(".")[-1]}')
                os.remove(f'{all_ps[i]}')
    
    if out_form == 'mmi':
        if len(glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/*.{extension}', recursive=True)) > 0:
            print(f'Converting the {extension} files from the {extension}_files directory to zip files')
            all_ps = glob.glob(f'output_ParallelPyMetaMap_{column_name}_{out_form}/{extension}_files/*.{extension}', recursive=True)
            for i in range(len(all_ps)):
                zipfile.ZipFile(f'{all_ps[i]}.zip', mode='w').write(f'{all_ps[i]}', arcname=f'{all_ps[i].split("/")[-1].split(".")[0]}.{all_ps[i].split("/")[-1].split(".")[-1]}')
                os.remove(f'{all_ps[i]}')
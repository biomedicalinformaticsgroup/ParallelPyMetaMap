import os

def output_files(column_name, out_form, extension):
    #creating the directories we are planning on using the save the result of the system
    if out_form == 'mmi':
        for path in [f'output_ParallelPyMetaMap_{column_name}_{out_form}',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df',
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
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/annotated_df',
                    f'output_ParallelPyMetaMap_{column_name}_{out_form}/temporary_df',
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
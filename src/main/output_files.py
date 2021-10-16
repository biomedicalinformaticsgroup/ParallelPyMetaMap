import os

def output_files(column_name, extension):
    #creating the directories we are planning on using the save the result of the system
    for path in [f'output_ParallelPyMetaMap_{column_name}',
                f'output_ParallelPyMetaMap_{column_name}/annotated_df',
                f'output_ParallelPyMetaMap_{column_name}/temporary_df',
                f'output_ParallelPyMetaMap_{column_name}/extra_resources',
                f'output_ParallelPyMetaMap_{column_name}/{extension}_files'
                ]:
        try:
            #try to create the directory, most likely will work for new project
            os.mkdir(path)
            print(f'Now creating {path}')
        except:
            #if the directory already exist just pass
            pass
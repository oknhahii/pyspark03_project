import get_env_variables as gev
import os
import logging

def get_dataset_info(src):
    dataset = []
    for file in os.listdir(src):
        file_format = file_path = header = inferSchema = ''

        file_path = os.path.join(src, file)

        if file.endswith('parquet'):
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        elif file.endswith('csv'):
            file_format = 'csv'
            header = gev.header
            inferSchema = gev.inferSchema

        logging.info('reading file which is of > {}'.format(file_format))

        dataset.append({
            'file_format': file_format,
            'file_path': file_path,
            'header': header,
            'inferSchema': inferSchema
        })
    return dataset
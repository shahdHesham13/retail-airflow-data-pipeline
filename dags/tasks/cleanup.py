import os

def delete_files(data_folder='/opt/airflow/data'):
    for filename in os.listdir(data_folder):
        file_path = os.path.join(data_folder, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
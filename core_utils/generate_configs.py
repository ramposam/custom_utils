import os

from core_utils.file_utils import read_and_infer, write_to_json_file
from core_utils.meta_classes import DatasetConfigs, DatasetVersion, DatasetMirror, DatasetStage
from pathlib import  Path


class ConfigTemplate():
    def __init__(self,bucket, file_path,start_date,datetime_format,catchup=False):
        self.file_path = file_path
        self.bucket = bucket
        self.start_date = start_date
        self.datetime_format = datetime_format
        self.catchup = catchup

    def get_mirror_schema(self, columns):
        schema = {}
        for col_name in columns:
            schema[col_name.replace(" ", "_").upper()] = "STRING"
        return schema

    def get_stage_schema(self, data_types):
        schema = {}
        for col_name, col_dtypes in data_types.items():
            schema[col_name.replace(" ", "_").upper()] = col_dtypes["snowflake_dtype"]
        return schema

    def generate_configs(self):
        delimiter, columns, data_types = read_and_infer(self.file_path)
        mirror_schema = self.get_mirror_schema(data_types)
        stage_schema = self.get_stage_schema(data_types)

        dataset_name = os.path.basename(os.path.dirname(self.file_path))

        configs_root_dir = os.path.join(os.getcwd(),"generated_configs")
        Path(configs_root_dir).mkdir(parents=True, exist_ok=True)

        dataset_dir = os.path.join(configs_root_dir,dataset_name)
        Path(dataset_dir).mkdir(parents=True, exist_ok=True)

        dataset_configs_path = os.path.join(dataset_dir,f"ds_{dataset_name}.json")
        ds_configs = DatasetConfigs(dataset_name=dataset_name,bucket=self.bucket,
                                    start_date=self.start_date,load_historical_data=self.catchup,
                                    snowflake_stage_name=f"STG_{dataset_name}".upper())

        write_to_json_file(data=ds_configs.__dict__,file_path=dataset_configs_path)

        dataset_mirror_dir = os.path.join(dataset_dir, "mirror")
        Path(dataset_mirror_dir).mkdir(parents=True, exist_ok=True)

        dataset_configs_mirror_ver_path = os.path.join(dataset_mirror_dir, f"ds_{dataset_name}_mirror_ver.json")
        ds_mirror_ver_configs = DatasetVersion(dataset_name=dataset_name)

        write_to_json_file(data=ds_mirror_ver_configs.__dict__, file_path=dataset_configs_mirror_ver_path)

        file_format = {
                    "delimiter": delimiter,
                    "skip_header": 1,
                    "compressed": True
                }
        dataset_configs_mirror_v1_path = os.path.join(dataset_mirror_dir, f"ds_{dataset_name}_mirror_v1.json")
        ds_mirror_v1_configs = DatasetMirror(table_name = f"T_ML_{dataset_name}".upper(),
                            table_schema = mirror_schema,
                            file_format = file_format,
                            file_schema = mirror_schema,
                            file_name_pattern = "datetime_pattern.csv",
                            file_path = f"/datasets/{dataset_name}",
                            datetime_pattern =  self.datetime_format)

        write_to_json_file(data=ds_mirror_v1_configs.__dict__, file_path=dataset_configs_mirror_v1_path)

        dataset_stg_dir = os.path.join(dataset_dir, "stage")
        Path(dataset_stg_dir).mkdir(parents=True, exist_ok=True)

        dataset_configs_stage_ver_path = os.path.join(dataset_stg_dir, f"ds_{dataset_name}_stage_ver.json")
        ds_stage_ver_configs = DatasetVersion(dataset_name=dataset_name)

        write_to_json_file(data=ds_stage_ver_configs.__dict__, file_path=dataset_configs_stage_ver_path)

        dataset_configs_stage_v1_path = os.path.join(dataset_stg_dir, f"ds_{dataset_name}_stage_v1.json")
        ds_stage_v1_configs = DatasetStage(table_name=f"T_STG_{dataset_name}".upper(),
                                             table_schema=stage_schema)

        write_to_json_file(data=ds_stage_v1_configs.__dict__, file_path=dataset_configs_stage_v1_path)


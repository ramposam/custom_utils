from core_utils.constants import snowflake_stage_template, snowflake_pipe_template
from core_utils.snowflake_utils import SnowflakeUtils


class SnowflakePipeline():
    def __init__(self, **kwargs):
        self.s3_bucket = kwargs.get("bucket")
        self.aws_access_key = kwargs.get("aws_access_key")
        self.aws_secret_key = kwargs.get("aws_secret_key")
        self.dataset_dir = kwargs.get("dataset_dir")
        self.dataset_name = kwargs.get("dataset_name")
        self.file_extension = kwargs.get("file_extension")
        self.delimiter = kwargs.get("delimiter")
        self.mirror_schema = kwargs.get("mirror_schema")
        self.stage_schema = kwargs.get("stage_schema")
        self.schedule_interval = kwargs.get("schedule_interval")
        self.warehouse = "COMPUTE_WH"

    def get_stage_sql(self):
        stage_sql = snowflake_stage_template.format(s3_bucket=self.s3_bucket,
                                                    dataset_name=self.dataset_name.upper(),
                                                    aws_access_key=self.aws_access_key,
                                                    aws_secret_key=self.aws_secret_key)
        return stage_sql

    def get_snowpipe_sql(self, copy_statement):
        copy_statement = ""

        snowflake_pipe_sql = snowflake_pipe_template.format(dataset_dir=self.dataset_dir,
                                                            dataset_name=self.dataset_name,
                                                            file_extension=self.file_extension,
                                                            copy_statement=copy_statement)
        return snowflake_pipe_sql

    def get_stream_sql(self, stream_name, table_name):
        stream_sql = f"""CREATE OR REPLACE STREAM {stream_name} ON TABLE {table_name};"""
        return stream_sql

    def get_task_sql(self, stream_name, task_name, tgt_table):
        task_sql = f"""
            CREATE OR REPLACE TASK {task_name}
            SCHEDULE = 'USING CRON {self.schedule_interval}'
            WAREHOUSE = '{self.warehouse}'
            AS
            INSERT INTO {tgt_table}
            SELECT *
            FROM {stream_name};

            ALTER TASK {task_name} RESUME;
            """
        return task_sql

    def get_all_sqls(self):

        dataset_name_upper = self.dataset_name.upper()
        table_name = f"T_ML_{dataset_name_upper}_TR"
        file_format_name = f"MIRROR_DB.MIRROR.ff_{dataset_name_upper}"
        stream_name = f"MIRROR_DB.MIRROR.STREAM_{dataset_name_upper}"
        task_name = f"MIRROR_DB.MIRROR.TASK_{dataset_name_upper}"
        tgt_table = f"T_ML_{dataset_name_upper}"

        stage_sql = self.get_stage_sql()

        util = SnowflakeUtils(
            stage_name=f"STG_{dataset_name_upper}",
            table_name=table_name)

        file_format_sql = util.get_file_format_sql(file_format_name=file_format_name,
                                                   delimiter=self.delimiter)
        if isinstance(self.mirror_schema, dict):
            columns = list(self.mirror_schema.keys())
        else:
            columns = []
        copy_statement = util.get_copy_into_table_sql(columns=columns, file_format_name=file_format_name)

        snowpipe_sql = self.get_snowpipe_sql(copy_statement)

        stream_sql = self.get_stream_sql(stream_name=stream_name, table_name=table_name)

        task_sql = self.get_task_sql(stream_name=stream_name, task_name=task_name, tgt_table=tgt_table)

        all_sqls = "\n".join([stage_sql, file_format_sql, snowpipe_sql, stream_sql])

        return all_sqls






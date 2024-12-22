import logging

from core_utils.constants import mirror_file_meta_cols


class SnowflakeUtils:

    def __init__(self, stage_name,table_name):
        self.stage_name = stage_name
        self.table_name = table_name

    def get_snowflake_stg_file_details(self):
        list_files_query = f"list @{self.stage_name}"
        with self.sf_conn.cursor() as cur:
            cur.execute(list_files_query)
            result = cur.fetchall()
            stage_file_name = result[0][0].split("/")[-1]
            file_name = stage_file_name.replace(".gz", "") if stage_file_name.endswith(".gz") else stage_file_name
            db_schema = ".".join(self.table_name.split(".")[:-1])
            file_format = f"""{db_schema}.ff_{self.stage_name.split(".")[-1][4:].lower().split('.')[0]}"""
            compression = "gzip" if stage_file_name != "csv" else "NONE"
            logging.info(f"Stage file:{stage_file_name}, file_format:{file_format},file_compression_type:{compression}")
        return stage_file_name,file_format,compression

    def get_file_format_sql(self,file_format_name, file_type="CSV", delimiter=",",skip_header=1, compression="NONE"):
        # Define the SQL command to create the file format

        file_format_sql = f"""
        CREATE OR REPLACE FILE FORMAT {file_format_name}
        TYPE = {file_type}
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = '{delimiter}'
        SKIP_HEADER = {skip_header}      
        TRIM_SPACE=TRUE,
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT='YYYY-MM-DD',
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
        COMPRESSION = {compression};
        """
        logging.info(f"File format sql: {file_format_sql}")
        return file_format_sql



    def get_copy_into_table_sql(self, columns,file_extension, file_format_name, file_path=None):

        cols_list_str = ",".join([f"${index+1} as {col_name.upper()}" for index, col_name in enumerate(columns)])

        meta_cols = []
        for col in mirror_file_meta_cols:
            meta_cols.append(f"metadata${col} as {col}")

        meta_cols_list_str = ",".join(meta_cols)

        # Define the SQL command to load data from the stage into the table
        copy_sql = f"""        
        COPY INTO {self.table_name}
        FROM (
            SELECT {cols_list_str},{meta_cols_list_str},
            current_timestamp as created_dts, current_user as created_by
            FROM '@{self.stage_name}'
        )
        # FILES = ('')
        PATTERN = '.*\.{file_extension}$'
        FILE_FORMAT = (FORMAT_NAME={file_format_name})
        FORCE = FALSE ;
        """

        logging.info(f"File format sql: {copy_sql}")
        return copy_sql



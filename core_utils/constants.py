snowflake_stage_template = """
CREATE OR REPLACE STAGE MIRROR_DB.MIRROR.STG_{dataset_name}_S3
  URL='s3://{s3_bucket}/{dataset_dir}/'
  CREDENTIALS=(AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key');
  """

snowflake_pipe_template = """
CREATE PIPE MIRROR_DB.MIRROR.PIPE_{dataset_name}
    AUTO_INGEST = TRUE 
    COPY_OPTIONS = (
        ON_ERROR = 'CONTINUE' -- Or 'ABORT_STATEMENT'
    )
AS
{copy_statement}
PATTERN = '.*\.{file_extension}';
"""
mirror_file_meta_cols = ["filename","file_row_number","file_last_modified"]
mirror_meta_cols = ["created_dts","created_by"]
stage_file_meta_cols = ["filename","file_row_number","file_last_modified"]
stage_meta_cols = ["created_dts","created_by","updated_dts","updated_by","active",
                   "effective_start_date", "effective_end_date","row_hash_id" ]

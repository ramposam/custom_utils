snowflake_stage_template = """
CREATE OR REPLACE STAGE MIRROR_DB.MIRROR.STG_{dataset_name}_S3
  URL='s3://{s3_bucket}/{s3_dataset_path}/'
  CREDENTIALS=(AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key');
  """

snowflake_pipe_template = """
CREATE OR REPLACE PIPE MIRROR_DB.MIRROR.PIPE_{dataset_name}
    AUTO_INGEST = TRUE     
AS {copy_statement} ;

-- To load all the available files under the path(Historical Data)
ALTER PIPE MIRROR_DB.MIRROR.PIPE_{dataset_name} REFRESH;

-- Create event notification on corresponding bucket using the arn.
-- Get the arn using below query.
-- Unless you create event notification, snowpipe is not going to copy data.
select  SYSTEM$PIPE_STATUS('MIRROR_DB.MIRROR.PIPE_{dataset_name}');

"""
mirror_file_meta_cols = ["filename","file_row_number","file_last_modified"]
mirror_meta_cols = ["created_dts","created_by"]
stage_file_meta_cols = ["filename","file_row_number","file_last_modified"]
stage_meta_cols = ["created_dts","created_by","updated_dts","updated_by","active",
                   "effective_start_date", "effective_end_date","row_hash_id" ]

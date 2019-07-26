from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_staging_copy = """COPY {staging_table} FROM {S3_song_address}
                     ACCESS_KEY_ID {access_key_id}
                     SECRET_ACCESS_KEY {secret_access_key}
                     region 'us-west-2'
                     FORMAT AS JSON {json_format}
                   """    
    
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 aws_cred_id="",
                 redshift_conn_id="",
                 table_name="",
                 s3_address="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.aws_cred_id = aws_cred_id  
        self.conn_id = redshift_conn_id
        self.table_name = table_name                  
        self.s3_address = s3_address
        

    def execute(self, context):
   
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_cred_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.conn_id)
 
        self.log.info('StageToRedshiftOperator staging_tables starting')
        if self.table_name == 'staging_songs':
            
            redshift_hook.run(self.sql_staging_copy.format(
                staging_table = self.table_name,
                S3_song_address = self.s3_address,
                access_key_id = credentials.access_key, 
                secret_access_key = credentials.secret_key,
                json_format = "'auto'"))
            self.log.info('StageToRedshiftOperator staging_songs_tables done')
        else:
            redshift_hook.run(self.sql_staging_copy.format(
                staging_table = self.table_name,
                S3_song_address = self.s3_address,
                access_key_id = credentials.access_key, 
                secret_access_key = credentials.secret_key,
                json_format = "'s3://udacity-dend/log_json_path.json'"))
            self.log.info('StageToRedshiftOperator staging_events_tables done')
                
        self.log.info('StageToRedshiftOperator staging_tables finished')





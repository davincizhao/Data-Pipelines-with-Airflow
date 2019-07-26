from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):
    
        
    ui_color = '#F98866'
    sql_insert_table="""INSERT INTO songplays (
                                        playid ,
                                        start_time ,
                                        userid,
                                        "level" ,
                                        songid ,
                                        artistid ,
                                        sessionid ,
                                        location ,
                                        user_agent 
                                      
                                    ) """
    statement_sql = (sql_insert_table + SqlQueries.songplay_table_insert)
    #print("sql_::::\n",statement_sql)
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = redshift_conn_id
       

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift_hook = PostgresHook(self.conn_id)
 
        self.log.info('LoadFactOperator  starting')
        redshift_hook.run(self.statement_sql)
        self.log.info('LoadFactOperator finished')
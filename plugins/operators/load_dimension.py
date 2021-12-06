from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table, 
                 sql,
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        self.log.info(f"Inserting values on {self.table} --- ")
        postgres_hook = PostgresHook(self.conn_id) 
         
        if not self.append_only:
            self.log.info(f"Truncating table {self.table}")
            postgres_hook.run( f" TRUNCATE TABLE {self.table} " )
        
        # Inserting data from staging table into dimension
        custom_sql = f"INSERT INTO {self.table} ({self.sql})"
        postgres_hook.run(custom_sql)        
        self.log.info(f"Success: Inserting values on {self.table}, {self.task_id} loaded.")

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator): 
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id, 
                 table, 
                 sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        postgres_hook = PostgresHook(self.conn_id)
        custom_sql = f"INSERT INTO {self.table} ({self.sql})"
        postgres_hook.run(custom_sql)
        self.log.info(f"Success: {custom_sql} __ ")

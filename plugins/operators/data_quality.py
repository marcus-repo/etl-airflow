from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    has_rows_sql = """
        SELECT COUNT(1) FROM {table};
    """
    
    has_null_sql = """
        SELECT COUNT(1) FROM {table} WHERE '{field}' is null;
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        #self.table = kwargs["params"]["table"]
        

    def execute(self, context):
        
        # connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # get records from redshift
        formatted_sql = DataQualityOperator.has_rows_sql.format(table=self.table)
        records = redshift.get_records(formatted_sql)
        
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
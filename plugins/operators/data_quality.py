from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_params="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_params=dq_params
        
        
    def has_rows(self, redshift):
        
        sql = """
            SELECT COUNT(1) FROM {table};
        """
        
        # log data quality test
        self.log.info("Data Quality Start: Record Count > 0")
        
        # loop through table names
        d = self.dq_params['has_rows']
        for table in d:
            
            # get records from redshift
            formatted_sql = sql.format(table=table)
            records = redshift.get_records(formatted_sql)
            
            # check result set
            if records is None or len(records[0]) < 1:
                raise ValueError("Data quality check failed. "\
                                 "{} returned no results".format(table))
            
            # check record count
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. "\
                                 "{} contained 0 rows".format(table))
            
            # data quality test passed
            self.log.info("Data quality on table {} check passed "\
                          "with {} records".format(table, num_records))       
        
    
    def has_nulls(self, redshift):
    
        sql = """
            SELECT COUNT(1) FROM {table} WHERE {field} IS NULL;
        """
        
        # log data quality test
        self.log.info("Data Quality Start: Check for NULLs")
        
        # loop through table and field names
        d = self.dq_params['has_nulls']
        for table in d:
            for field in d[table]:
                print(table, field)
                
                # get records from redshift
                formatted_sql = sql.format(table=table,field=field)
                records = redshift.get_records(formatted_sql)
                
                # check nulls
                num_records = records[0][0]
                if num_records > 0:
                    raise ValueError("Data quality check failed. "\
                                     "{table}.{field} contained "\
                                     "{num_records} NULL rows"
                                     .format(table=table,
                                             field=field,
                                             num_records=num_records))
                
                # data quality test passed
                self.log.info("Data quality on table.field {table}.{field} "\
                              "check passed with {num_records} NULL rows"
                              .format(table=table, 
                                      field=field,
                                      num_records=num_records))     
                

    def execute(self, context):
        
        # connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        

        # execute has_rows check
        if 'has_rows' in self.dq_params:
            self.has_rows(redshift)
        
        # execute has_nulls check
        if 'has_nulls' in self.dq_params:
            self.has_nulls(redshift)

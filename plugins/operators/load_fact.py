from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {} ({});
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 clear_table=False,
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.clear_table=clear_table
        self.sql=sql


    def execute(self, context):
        
        # connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clean redshift destination
        if self.clear_table:
            self.log.info("Clearing data from destination Redshift table {}"
                          .format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
            
        # insert data from staging to fact
        self.log.info("Inserting data from Redshift staging "\
                      "to destination table {}".format(self.table))
        
        # format sql using insert_sql
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
        
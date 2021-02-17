from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 clear_table=False,
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.clear_table = clear_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        

    def execute(self, context):
        
        # retrieve aws credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear redshift destination
        if self.clear_table:
            self.log.info("Clearing data from destination Redshift table {}"
                          .format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        # copy data from S3 to redshift
        self.log.info("Copying data from S3 to Redshift table {}"
                      .format(self.table))
        
        # render s3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(s3_path)
        
        # render jsonpath if not "auto"
        if self.json_path != "auto":
            json = "s3://{}/{}".format(self.s3_bucket, self.json_path)
        else:
            json = self.json_path
        
        # format sql using copy_sql
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json
        )
        redshift.run(formatted_sql)






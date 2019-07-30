import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'table',
        'database',
        'destination_bucket'
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data Catalog : Database and Table Name
db_name = args['database']
tbl_name = args['table']

# Output Details
output_path = "s3://" + args['destination_bucket']

## @type: DataSource
## @args: [database = db_name, table_name = tbl_name, transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = db_name, 
    table_name = tbl_name, 
    transformation_ctx = "datasource0"
)

## @type: ApplyMapping
## @args: [mapping = [("col0", "long", "timestamp", "long"), ("col29", "double", "amount", "double"), ("col30", "double", "latitude", "double"), ("col31", "double", "longitude", "double"), ("col32", "long", "fraud", "long"), ("col33", "double", "score", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(
    frame = datasource0, 
    mappings = [("col0", "long", "timestamp", "long"), ("col29", "double", "amount", "double"), ("col30", "double", "latitude", "double"), ("col31", "double", "longitude", "double"), ("col32", "long", "fraud", "long"), ("col33", "double", "score", "double")], 
    transformation_ctx = "applymapping1"
)

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": output_path}, format = "parquet", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(
    frame = applymapping1, 
    connection_type = "s3", 
    connection_options = {
        "path": output_path
    }, 
    format = "parquet", 
    transformation_ctx = "datasink2"
)
job.commit()
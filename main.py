import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_timestamp
from pyspark.sql.types import StringType
from uuid import uuid4
from pyspark.sql.functions import udf

if __name__ == '__main__':

    """
    Running arguments: read_path, withHeader(or withoutHeader), output_path
    """

    if len(sys.argv) != 3:
        print("Usage: HomeTest <file>", file=sys.stderr)
        sys.exit(-1)

    # Ingest files with and without header
    header = True if sys.argv[2] == 'withHeader' else False

    spark = SparkSession \
        .builder \
        .appName('HomeTest') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Read csv files from path
    df = spark.read.csv(sys.argv[1], header=header)

    # Add extra columns - ingestion_tms and batch_id
    batch_id = udf(lambda: str(uuid4()), StringType())
    output = df \
        .withColumn('ingestion_tms', date_format(current_timestamp(), 'YYYY-MM-DD HH:mm:SS')) \
        .withColumn('batch_id', batch_id())

    # Write to Delta table
    output.write.mode('append').format('delta').save(sys.argv[3])

    spark.stop()

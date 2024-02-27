import logging.config
import os
import get_env_variables as gav

logger = logging.getLogger('Ingest')

def load_files(spark, file_dir, file_format, header, inferSchema):
    df_rdd = None
    try:
        logger.warning("load_files method started")

        if file_format == 'parquet':
            df_rdd = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv':

            df_rdd = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(file_dir)
        elif file_format == 'txt':
            df_rdd = spark.sparkContext.textFile(file_dir)

    except Exception as e:
        logger.error("An error occurred while dealing with load_file ====" + str(e))
        raise
    else:
        logger.warning("Load_files func done, go fwd..")
    return df_rdd


def display_df(df, df_name):
    df_showing = df.show()
    return df_showing

def df_count(df,df_name):
    try:
        dc = df.count()
    except Exception as ex:
        raise
    else:
        logging.info('Number of records of {} table is {} '.format(df_name,dc))

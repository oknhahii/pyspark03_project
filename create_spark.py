import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging.config
import get_env_variables as gev

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Create_spark')


def get_spark_session(envn, appName):
    try:

        if envn == 'DEV':
            master = 'local[*]'

        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))
        conf = SparkConf()
        conf.set("spark.jars", gev.ps_driver)
        spark = SparkSession.builder.config(conf=conf.appName(appName).enableHiveSupport().getOrCreate()
    ).master(master)
    except Exception as exp:
        logger.error('An error occurred in the get_spark_object==== ', str(exp))
        raise

    else:
        logger.info('Spark object created.....')
        return spark
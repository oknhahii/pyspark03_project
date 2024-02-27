from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType= IntegerType())

def column_split_count(column):
    return len(column.split(' '))

def count_the_word(rdd):
    rdd1 = rdd.flatMap(lambda line: line.split(' '))

    rdd2 = rdd1.flatMap(lambda line: line.split(' '))
    return rdd2.count()

def get_target(rdd):
    return rdd.first()

from pyspark.sql.types import *

label_schema=StructField(name='label',dataType=FloatType(),nullable=True)
num_schema=[StructField(name='col_{}'.format(i),dataType=FloatType(),
                            nullable=True) for i in range(1,14)]
cat_schema=[StructField(name='col_{}'.format(i),dataType=StringType(),
                            nullable=True) for i in range(14,40)]
TABLE_SCHEMA=StructType([label_schema]+num_schema+cat_schema)
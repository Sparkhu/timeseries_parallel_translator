#!/usr/bin/env python
# coding: utf-8

import findspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.functions import concat, col, lit, concat_ws

findspark.init()

conf = SparkConf().setMaster("yarn").setAppName("timeseries_parallel_translator")
sc = SparkContext.getOrCreate(conf)
sqlContext = SQLContext(sc)


# ### Read file
MEASUREMENT = "electricity"
TAG_KEY = "zone"
TAG_VALUE = "FGE"

elec_FGE_df = sqlContext.read.format("com.databricks.spark.csv") .option("inferSchema", 'True') .option("header", True) .load("AMPds2/Electricity_FGE.csv")
elec_FGE_df = elec_FGE_df.drop("Pt", "Qt", "St")

#from pyspark.sql.functions import max, min, avg, stddev, count
#exprs = {x: "min" for x in elec_FGE_df.columns if x is not elec_FGE_df.columns[0]}
# elec_FGE_df.agg(exprs).show()

#columns = [x for x in elec_FGE_df.columns if x is not elec_FGE_df.columns[0]]
#elec_FGE_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in columns]).show()

time_translation_parallel_udf = udf(lambda x: x + 3196*24*60*60)
mock_df = elec_FGE_df.withColumn("unix_ts",time_translation_parallel_udf(elec_FGE_df.unix_ts))
#mock_df.show()

#mock_df.tail(1)


# concatenate for influxdb points
mock_df.select(concat(lit(MEASUREMENT + "," + TAG_KEY + "=" + TAG_VALUE + " "),
                     concat_ws(',', *[concat(lit(c), lit("="), col(c).cast('string')) for c in mock_df.columns if c!='unix_ts']),
                     lit(" "), col('unix_ts').cast('string')).alias('cat'))\
.write.option("header", False).format("text").mode('overwrite').save("mock_data.txt")

sc.stop()


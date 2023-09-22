from typing import cast

from pyspark.sql import functions as sf, DataFrame

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

df.where(df['authors'].like('%Iain%Banks%')).orderBy('original_publication_year').show()

# %% Exercise 01: How many distinct authors are there?
# Please use an aggregation and not the df.distinct() method.
df.select(sf.countDistinct(df.authors)).show()
# 4664



# %% Exercise 02: which 10 authors have the most books in the sample
df_by_author = df.groupby(df.authors)

df_by_author.count().orderBy(sf.desc('count')).limit(10).show()
df_by_author.agg(sf.count('*').alias('count')).orderBy(sf.desc('count')).limit(10).show()


# %% Exercise 03: What is the mean, maximum and minimum average_rating
# for these authors and the earliest original_publication_year.

df_by_author.agg(sf.count('*').alias('count'),
                 sf.mean(df.average_rating),
                 sf.max(df.average_rating),
                 sf.min(df.average_rating),
                 sf.min(df.original_publication_year)
                 ).orderBy('count', ascending=False).limit(10).show()

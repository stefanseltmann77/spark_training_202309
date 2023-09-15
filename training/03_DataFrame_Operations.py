import datetime

from pyspark.sql.types import StringType, IntegerType

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: How many rows of data are there?
df.count()
# 10.000

pdf = df.toPandas()


# %% Exercise 02: Look at the Schema. Extract the datatypes,
# that are of type integer.
# Make a list of the column names of integers

cols_int = [name for name, dtype in df.dtypes if dtype == 'int']
cols_int

# %% Exercise 03: select the columns isbn13 and original_publication_year.
# What types are they? Cast them to a more appropriate formats

df.select("original_publication_year", "isbn13").dtypes

# 3.3
df = df.withColumn("isbn13", df["isbn13"].cast(StringType()))
df = df.withColumn("original_publication_year", df["original_publication_year"].cast(IntegerType()))

#3.4
col_map = {"isbn13": df["isbn13"].cast(StringType()),
           "original_publication_year": df["original_publication_year"].cast(IntegerType())}
df = df.withColumns(col_map)

# %% Exercise 04: Drop the two columns that contain urls

df.columns

url_cols = [name for name in df.columns if name.endswith("url")]
df = df.drop(*url_cols)

# %% Exercise 05: Create new columns,
# - Set the current date in a column: work_dt
# - top_rating_flg, if average_rating above 2.0 or not
# - contemporary_flg, if original_publication_year greater than 2000
# commands: "case when" ->  when().otherwise()
import pyspark.sql.functions as sf

col_map = {'work_dt': sf.current_timestamp(),
           'work_dt': sf.lit(datetime.datetime.now()),
           'top_rating_flg': sf.when(sf.col('average_rating') > 2.0, 1).otherwise(0),
           'contemporary_flg': sf.when(sf.col('original_publication_year') > 2000, 1).otherwise(0)}

df = df.withColumns(col_map)
df.show()

# %% Exercise 06: Check if there are any missings
df.describe().toPandas().transpose()[0]

df.columns

null_cols = [sf.sum(sf.when(sf.col(name).isNull(), 1).otherwise(0)).alias("nulls_"+name)
             for name in df.columns]
df.select(*null_cols).show()



# %% Exercise 07: How many rows are left, if we discard all missings?

df.count() - df.dropna().count()
2140
# %% Exercise 08: Which books have no language_code? What are the original titles.

df.where("language_code is null").select("original_title").show()
df.filter(df["language_code"].isNull()).select("original_title").show()
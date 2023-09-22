from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)


# %% Exercise 01: Read the ratings.csv, tags.csv, book_tags.csv and get familiar with the columns.
# What columns are there in each dataset?
csv_reader = spark.read.options(**{'header': True, 'inferSchema': True})

df_books = df
df_ratings = csv_reader.csv("./training_data/ratings.csv")
df_tags = csv_reader.csv("./training_data/tags.csv")
df_book_tags = csv_reader.csv("./training_data/book_tags.csv")

df_books.columns
df_ratings.columns
df_tags.columns
df_book_tags.columns

# %% Exercise 02: Check all four DataFrames for columns to join with.
set(df_books.columns).intersection(df_ratings.columns)
set(df_book_tags.columns).intersection(df_tags.columns)
set(df_books.columns).intersection(df_book_tags.columns)

# %% Exercise 03: Take the book table and join it first with the ratings. What is the count?
df_joined = df_books.join(df_ratings, on='book_id', how='inner').cache()
df_joined.count()
79701

# %% Exercise 04: Which User (by user_id) rated the most books?
top_user = (df_joined.
            groupby('user_id').
            count().orderBy('count', ascending=False).limit(2))
top_user.show()

[row[0] for row in top_user.collect()]

# +-------+-----+
# |user_id|count|
# +-------+-----+
# |  11927|   32|
# |  23612|   32|
# +-------+-----+
# %% Exercise 05: What are the frequent tags for the books of this user?
# Display the labels


df =  (df_joined.where(f"user_id in ('11927', '23612)").
       join(df_book_tags, on=df_books.book_id == df_book_tags.goodreads_book_id).
       join(df_tags, on='tag_id').
       groupby('tag_name').count().orderBy('count', ascending=False))

df =  (df_joined.where(f"user_id in {tuple([row[0] for row in top_user.collect()])}").
       join(df_book_tags, on=df_books.book_id == df_book_tags.goodreads_book_id).
       join(df_tags, on='tag_id').
       groupby('tag_name').count().orderBy('count', ascending=False))



df =  (df_joined.
       join(top_user, on='user_id', how='leftsemi').
       join(df_book_tags, on=df_books.book_id == df_book_tags.goodreads_book_id).
       join(df_tags, on='tag_id').
       groupby('tag_name').count().orderBy('count', ascending=False))

spark.conf.set("spark.sql.adaptive.enabled", "false")  # make, sure it is enabled
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # legacy, raise if too low
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")   # raise if too low

df_solution\
    = df.cache()

df_solution.unpersist()

df_solution.show()

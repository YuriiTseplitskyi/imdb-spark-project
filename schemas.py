from pyspark.sql import SparkSession
from pyspark.sql.types import(
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    FloatType, 
    BooleanType
)

spark = SparkSession.builder.appName("IMDB Data").getOrCreate()

title_basics_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
])

title_ratings_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", FloatType(), True),
    StructField("numVotes", IntegerType(), True)
])

title_crew_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])

title_akas_schema = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", BooleanType(), True)
])

title_episode_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(), True),
    StructField("episodeNumber", IntegerType(), True)
])

title_principals_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
])

name_basics_schema = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])

data_frames = {
    "title_basics": spark.read.option("delimiter", "\t").schema(title_basics_schema).csv("/data/imdb/title.basics.tsv.gz"),
    "title_ratings": spark.read.option("delimiter", "\t").schema(title_ratings_schema).csv("/data/imdb/title.ratings.tsv.gz"),
    "title_crew": spark.read.option("delimiter", "\t").schema(title_crew_schema).csv("/data/imdb/title.crew.tsv.gz"),
    "title_akas": spark.read.option("delimiter", "\t").schema(title_akas_schema).csv("/data/imdb/title.akas.tsv.gz"),
    "title_episode": spark.read.option("delimiter", "\t").schema(title_episode_schema).csv("/data/imdb/title.episode.tsv.gz"),
    "title_principals": spark.read.option("delimiter", "\t").schema(title_principals_schema).csv("/data/imdb/title.principals.tsv.gz"),
    "name_basics": spark.read.option("delimiter", "\t").schema(name_basics_schema).csv("/data/imdb/name.basics.tsv.gz")
}
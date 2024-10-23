Problem Statement
●	Books data analysis

Use Cases
●	Create DF on json
○	
df= spark.read.json('books.json')
○	
●	Counts the number of rows in dataframe
●	Counts the number of distinct rows in dataframe
●	Remove Duplicate Values
●	Select title and assign 0 or 1 depending on title when title is not odd ODD HOURS and assign this value to a column named as newHours
○	
from pyspark.sql.functions import *
df.select("title", when(df.title != 'ODD HOURS', 1).otherwise(0).alias("newHours")).show(10)
●	Select author and title is TRUE if title has "THE" word in titles and assign this value to a column named as universal
●	Select substring of author from 1 to 3 and alias as newTitle1
○	
df.select(df.author.substr(1, 3).alias("newTitle1")).show(5)

●	Select substring of author from 3 to 6 and alias as newTitle2
●	
●	Show and Count all entries in title, author, rank, price columns
●	Show rows with for specified authors 
○	"John Sandford", "Emily Giffin"
○	
●	Select "author", "title" when 
○	title startsWith "THE"
○	title endsWith "IN"
●	Update column 'amazon_product_url' with 'URL'
●	Drop columns publisher and published_date
○	
df.drop("publisher", "published_date").show(5)

○	
●	Group by author, count the books of the authors in the groups
●	Filtering entries of title Only keeps records having value 'THE HOST'
●	
Submission
●	Pyspark code



***************************************************************************************************************************************************************************************

Solution

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def book_data_analysis():
    spark = SparkSession.builder.appName("BookData Analysis").getOrCreate()
    df = spark.read.json('file:///home/takeo/pycharmprojects/datacleaning/SparkRDD')

    #To Count Rows in Dataframe
    row_count=df.count()

    # To count distinct row in dataframe
    distinct_count = df.distinct().count()

    # To remove duplicate values
    df_no_duplicates = df.dropDuplicates()

    #●	Select title and assign 0 or 1 depending on title when title is not odd ODD HOURS and assign this value to a column named as newHours
    df_odd = df_no_duplicates.select("title", when(df.title != 'ODD HOURS', 1).otherwise(0).alias("newHours"))

    #●	Select author and title is TRUE if title has "THE" word in titles and assign this value to a column named as universal
    df_the = df_no_duplicates.withColumn("universal", df_no_duplicates.title.contains("THE"))
    df_true_universal = df_the.filter(col("universal")==True)
    df_universal=df_true_universal.select("author", "title","universal")
    #print(df_universal.show(10))

    # Select substring of author from 1 to 3 and alias as newTitle1
    df_newTitle1 = df.select(df.author.substr(1,3).alias("newTitle1"))
    #print(df_newTitle1.show(10))

    # Select substring of author from 3 to 6 and alias as newTitle2
    df_newTitle2 = df.select(df.author.substr(3,6).alias("newTitle2"))
    #print(df_newTitle2.show(10))

    # Show and Count all entries in title, author, rank, price columns
    df_selected_columns = df.select("title", "author", "rank", "price")
    #print(df_selected_columns.show(10))
    selected_column_count = df_selected_columns.count()
    #print(selected_column_count)

    #●	Show rows with for specified authors "John Sandford", "Emily Giffin"
    df_specified_author =df.filter(df.author.isin("John Sandford", "Emily Giffin"))
    #print(df_specified_author.show(10))

    #●	Select "author", "title" when  title startsWith "THE"  title endsWith "IN"
    df_author = df.select("author","title").filter(col("title").startswith("THE") & col("title").endswith("IN"))
    #print(df_author.show())

    # ●	Update column 'amazon_product_url' with 'URL'
    df_url = df.withColumnRenamed("amazon_product_url", "URL")
    #print(df_url.show())

    #●	Drop columns publisher and published_date
    #print(df.drop("publisher", "published_date").show(5))

    #●	Group by author, count the books of the authors in the groups
    df_group_by_author =df.groupBy("author").count()
    #print(df_group_by_author.show())

    #Tried with Pysql
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("books")

    # SQL query to group by author and count the number of books for each author
    df_grouped_by_author_sql = spark.sql("""
        SELECT author, COUNT(*) as book_count
        FROM books
        GROUP BY author
    """)

    # Show the result
    #print(df_grouped_by_author_sql.show())

    # Filtering entries of title Only keeps records having value 'THE HOST'
    df_entries = df.filter(col("title").contains("THE HOST"))
    print(df_entries.show(5))

if __name__ == '__main__':
    bookdata_analysis()


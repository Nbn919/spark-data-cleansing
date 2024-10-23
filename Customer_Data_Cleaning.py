Customer Data Cleaning
Problem Statement
●	As a data engineer you are given the task to clean the customer data
●	Your pipeline should remove all the duplicates records
●	And also remove those records which are duplicated on the basis of height and age

Data

Smith,23,5.3
Rashmi,27,5.8
Smith,23,5.3
Payal,27,5.8
Megha,27,5.4
Metadata- columns

Name - String
Age - Integer
Height - double

***************************************************************************************************************************************************************************************


from pyspark.sql import SparkSession

def customer_data():
    # Define the sample data
    data = [
        ("Smith", 23, 5.3),
        ("Rashmi", 27, 5.8),
        ("Smith", 23, 5.3),
        ("Payal", 27, 5.8),
        ("Megha", 27, 5.4)
    ]

    # Define the column names
    columns = ["Name", "Age", "Height"]

    spark = SparkSession.builder.appName("Customer Data Cleaning").getOrCreate()

    # Create a DataFrame
    df = spark.createDataFrame(data, columns)
    df_distinct = df.dropDuplicates()
    df_no_duplicates = df_distinct.dropDuplicates(['Age', 'Height'])
    print(df_no_duplicates.show())

if __name__ == '__main__':
    customer_data()

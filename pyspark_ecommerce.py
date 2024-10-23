PySpark Business Use Cases in the E-commerce Sector
This document presents business use cases for PySpark in the e-commerce sector using HDFS as the source and Hive as the target. We will explore data cleaning techniques such as removing duplicates, handling null values, and applying other popular data cleansing methods. The sample data involves multiple entities, and we will transform this data into multiple tables in Hive using PySpark code.
1. Business Use Cases
1.1 Customer Data Integration
**Description**: Integrating customer data from various sources to create a unified and consistent dataset. This involves handling missing values, removing duplicates, standardizing data formats, and ensuring data completeness.
1.2 Sales Transactions Analysis
**Description**: Processing sales transaction data to identify purchase patterns, trends, and customer behaviors. This requires transforming raw sales data, handling inconsistencies, and merging it with other e-commerce data to create an analytical dataset.
2. Sample Data
2.1 Customer Data (HDFS Source)
**Columns**: customer_id, first_name, last_name, email, phone, address, registration_date, last_purchase_date, total_spent
**Sample Records**:
1, John, Doe, john.doe@example.com, 123-456-7890, 123 Elm St, 2023-01-10, 2023-06-15, 500.00
2, Jane, Smith, jane.smith@example.com, NULL, 456 Pine St, 2023-02-05, NULL, 0.00
2, Jane, Smith, jane.smith@example.com, NULL, 456 Pine St, 2023-02-05, NULL, 0.00
3, NULL, Brown, mary.brown@example.com, 789-123-4560, NULL, 2023-03-20, 2023-04-25, 200.00
2.2 Sales Transactions Data (HDFS Source)
**Columns**: transaction_id, customer_id, product_id, quantity, price, transaction_date, payment_method
**Sample Records**:
101, 1, P001, 2, 50.00, 2023-06-10, Credit Card
102, 2, P002, 1, 100.00, 2023-06-11, PayPal
103, 2, P002, 1, 100.00, 2023-06-11, PayPal
104, 3, P003, 3, NULL, 2023-06-12, Credit Card


***********************************************************************************************************************************************************

Solution




from pyspark import SparkContext

def pyspark_ecommerce():
    data = [
        "1,101,5001,Laptop,Electronics,1000.0,1",
        "2,102,5002,Headphones,Electronics,50.0,2",
        "3,101,5003,Book,Books,20.0,3",
        "4,103,5004,Laptop,Electronics,1000.0,1",
        "5,102,5005,Chair,Furniture,150.0,1"
    ]
    product_category_data = [
        ('Laptop', 'Electronics'),
        ('Headphones', 'Electronics'),
        ('Book', 'Books'),
        ('Chair', 'Furniture')
    ]

    # Setup Spark Context
    sc = SparkContext("local", "E-Commerce Analysis")

    # Create RDDs
    transactions_rdd = sc.parallelize(data)
    transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))

    # Parallelizing product_category_data
    product_category_rdd = sc.parallelize(product_category_data)

    # Transformations
    high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)
    products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])

    # Customer Spending (customer_id, total_spent)
    pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))
    customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)

    # Products Per Customer (customer_id, list_of_products)
    customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)

    # Join Product Category Information
    product_rdd = pair_rdd.map(lambda x: (x[1][0], (x[0], x[1][1])))
    customer_product_category_rdd = product_rdd.join(product_category_rdd)

    # Actions
    total_spending = customer_spending_rdd.collect()
    products_per_customer = customer_products_rdd.collect()
    product_category_info = customer_product_category_rdd.collect()

    # Output to files (commented out if not needed)
    # customer_spending_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/customer_spending")
    # customer_products_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/customer_products")
    customer_product_category_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/customer_product_category")

    # Stop SparkContext
    sc.stop()

if __name__ == '__main__':
    pyspark_ecommerce()

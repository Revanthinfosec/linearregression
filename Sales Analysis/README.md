# README

## Overview
This project demonstrates how to perform various data analysis tasks using PySpark within a Databricks environment. It covers data loading, preprocessing, and generating various insights from sales and product data.

## Files
- `/FileStore/tables/sales_csv.txt`: Sales data
- `/FileStore/tables/menu_csv.txt`: Product menu data

## Schemas
### Sales Data Schema
- `product_id`: Integer
- `customer_id`: String
- `order_date`: Date
- `location`: String
- `source_order`: String

### Product Data Schema
- `product_id`: Integer
- `product_name`: String
- `price`: String

## Steps
1. **Load Data:**
   ```python
   schema = StructType([
       StructField("product_id", IntegerType(), True),
       StructField("customer_id", StringType(), True),
       StructField("order_date", DateType(), True),
       StructField("location", StringType(), True),
       StructField("source_order", StringType(), True),
   ])
   sales_df = spark.read.format("csv").option("inferschema", "true").schema(schema).load("/FileStore/tables/sales_csv.txt")
   display(sales_df)
   ```

2. **Preprocess Data:**
   Add `order_year`, `order_month`, and `order_quarter` columns.
   ```python
   from pyspark.sql.functions import month, year, quarter
   sales_df = sales_df.withColumn("order_year", year(sales_df.order_date))
   sales_df = sales_df.withColumn("order_month", month(sales_df.order_date))
   sales_df = sales_df.withColumn("order_quarter", quarter(sales_df.order_date))
   display(sales_df)
   ```

3. **Load Product Data:**
   ```python
   schema = StructType([
       StructField("product_id", IntegerType(), True),
       StructField("product_name", StringType(), True),
       StructField("price", StringType(), True)
   ])
   product_df = spark.read.format("csv").option("inferschema", "true").schema(schema).load("/FileStore/tables/menu_csv.txt")
   display(product_df)
   ```

4. **Analysis:**
   - **Total Money Spent by Customers:**
     ```python
     total_money_spent = (sales_df.join(product_df, 'product_id')
                          .groupBy('customer_id').agg({'price':'sum'})
                          .orderBy('customer_id'))
     display(total_money_spent)
     ```
   - **Total Money Spent by Product:**
     ```python
     total_money_spent = (sales_df.join(product_df, 'product_id')
                          .groupBy('product_name').agg({'price':'sum'})
                          .orderBy('product_name'))
     display(total_money_spent)
     ```
   - **Monthly Sales:**
     ```python
     monthsales_df = (sales_df.join(product_df, 'product_id')
                      .groupBy('order_month').agg({'price':'sum'}))
     display(monthsales_df)
     ```
   - **Yearly Sales:**
     ```python
     yearsales_df = (sales_df.join(product_df, 'product_id')
                     .groupBy('order_year').agg({'price':'sum'})
                     .orderBy('order_year'))
     display(yearsales_df)
     ```
   - **Quarterly Sales:**
     ```python
     quartersales_df = (sales_df.join(product_df, 'product_id')
                        .groupBy('order_quarter').agg({'price':'sum'})
                        .orderBy('order_quarter'))
     display(quartersales_df)
     ```
   - **Top Products Sold:**
     ```python
     from pyspark.sql.functions import count
     daily_df = (sales_df.join(product_df, 'product_id')
                 .groupBy('product_id', 'product_name')
                 .agg(count('product_id').alias('product_count'))
                 .orderBy('product_count', ascending=0)
                 .drop('product_id'))
     display(daily_df)
     ```

5. **Customer Analysis:**
   - **Distinct Orders by Restaurant Customers:**
     ```python
     from pyspark.sql.functions import countDistinct
     df = (sales_df.filter(sales_df.source_order == 'Restaurant')
           .groupBy('customer_id').agg(countDistinct('order_date')))
     display(df)
     ```
   - **Total Money Spent by Location:**
     ```python
     total_money_spent = (sales_df.join(product_df, 'product_id')
                          .groupBy('location').agg({'price':'sum'}))
     display(total_money_spent)
     ```
   - **Total Money Spent by Source Order:**
     ```python
     total_money_spent = (sales_df.join(product_df, 'product_id')
                          .groupBy('source_order').agg({'price':'sum'}))
     display(total_money_spent)
     ```

## Conclusion
This project provides a comprehensive example of using PySpark for data analysis within Databricks. It includes data loading, preprocessing, and performing various aggregation and analysis tasks to derive meaningful insights.

For more information, refer to the [original guide](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3941816308585215/1096661849344955/1582151743185315/latest.html).

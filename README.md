## Project Overview
This project focuses on data integration and analysis using MySQL and Eclipse IDE. It involves working with customer transactions, products, and metadata to perform a mesh join operation, followed by OLAP (Online Analytical Processing) queries for in-depth analysis. The goal is to identify trends, generate reports, and gain insights into customer behavior and product performance.

## Key Objectives
#### Data Loading and Integration
Load and preprocess data from CSV files into MySQL and use the mesh join technique to combine transaction data with metadata.

#### OLAP Analysis
Perform trend analysis, revenue analysis, and seasonal analysis using OLAP queries.

#### Technology Stack
Utilize MySQL for database management, Eclipse IDE for Java development, and JDBC for connectivity.

#### Meshjoin Algorithm
Implement the Meshjoin algorithm to efficiently process large datasets and perform joins in a memory-efficient manner.

#### Output
Store transformed data in a Data Warehouse (DW) and present query results in tabular format for business analysis.

## Schema Overview
The project uses a star schema optimized for OLAP queries. Key tables include:

- **Customers Table (customers_data):** Stores customer details like customer_id, customer_name, and gender.
- **Products Table (products_data):** Contains product information such as productID, productName, and productPrice.
- **Transactions Table (transactions):** Records transaction details like Order_ID, Order_Date, and Quantity_Ordered.
- **Transformed Data Table (transformed_data):** Enriched dataset combining transaction data with customer and product metadata.

## Meshjoin Algorithm Steps
1. **Read Data:** Load a segment of transaction data into a hash table.
2. **Load Metadata:** Load customer and product data into memory partitions.
3. **Join Operation:** Perform joins between metadata and transaction data.
4. **Enrich Data:** Add attributes like customer name and product price to transaction tuples.
5. **Load to Data Warehouse:** Insert enriched data into the DW without duplication.

## OLAP Queries
The project includes 10 OLAP queries, such as:

- Identifying top revenue-generating products.
- Analyzing quarterly revenue growth.
- Studying product affinity and seasonal trends.

## Shortcomings of Meshjoin
- **Limited to Key-based Joins:** Efficient only for joins based on keys like customer_id or productID.
- **Memory Usage:** High memory consumption for very large datasets.
- **Processing Speed:** Additional complexity may slow down processing for smaller datasets.

## Setup Instructions
### Prerequisites
- Eclipse IDE
- MySQL Database
- MySQL Connector/J (JDBC driver)

### Steps to Connect Eclipse to MySQL
1. Open Eclipse and go to your project.
2. Navigate to `Window > Perspective > Open Perspective > Others`, then select **Database Connections**.
3. Right-click on **Database Connections**, select **New**, and choose **MySQL**.
4. Click **New Driver Definition**, select **MySQL 5.1**, and switch to the **Jar List** tab.
5. Remove the default JAR, click **Add JAR/Zip**, and locate the MySQL Connector/J JAR file.
6. Test the connection and click **Finish** once successful.

### Creating a Java Project in Eclipse
1. Go to `File > New > Project`, select **Java Project**, name your project, and click **Finish**.
2. Right-click on the `src` folder, select `New > Class`, and create a new Java class.

### Adding MySQL Connector JAR to Build Path
1. Right-click on the project and select `Build Path > Configure Build Path`.
2. In the **Libraries** tab, click **Add External JARs**, select the MySQL Connector/J JAR file, and click **Apply and Close**.

### Running the Project
1. Go to `Run > Run As > Java Application`.
2. View the output in the **Console**.

## What I Learned
- Setting up MySQL Connector/J in Eclipse for JDBC connectivity.
- Optimizing large dataset processing using the Meshjoin algorithm.
- Executing SQL queries directly from Eclipse.
- Ensuring data quality by handling missing values and inconsistencies.

## Repository Structure
- `README.md`: This file.
- `Java source file`: Located in the `src` folder.
- `CSV data files`: `customers_data.csv`, `products_data.csv`, `transactions.csv`.

## License
This project is open-source and available under the MIT License.

package com.meshjoin;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.io.*;


public class Mesh_Join{
	
	public static Connection getConnection() {
	        // Initialize Scanner to get input from the user
	        Scanner scanner = new Scanner(System.in);

	        // Prompt for the base URL of the MySQL database (host and port)
	        System.out.print("Enter MySQL database base URL (e.g., localhost:3306): ");
	        String baseUrl = scanner.nextLine();

	        // Ask for the database name
	        System.out.print("Enter MySQL database name: ");
	        String dbName = scanner.nextLine();

	        // Ask for the username
	        System.out.print("Enter MySQL username: ");
	        String username = scanner.nextLine();

	        // Ask for the password
	        System.out.print("Enter MySQL password: ");
	        String password = scanner.nextLine();

	        // Close the scanner as it's no longer needed
	        scanner.close();

	        // Construct the full URL for database connection
	        String url = "jdbc:mysql://" + baseUrl + "/" + dbName + "?useSSL=false&serverTimezone=UTC";

	        try {
	            // Ensure MySQL JDBC driver is loaded
	            Class.forName("com.mysql.cj.jdbc.Driver");

	            // Establish the connection using the provided credentials
	            return DriverManager.getConnection(url, username, password);

	        } catch (ClassNotFoundException e) {
	            System.err.println("MySQL JDBC Driver not found. Please ensure it's in your classpath.");
	            e.printStackTrace();
	        } catch (SQLException e) {
	            System.err.println("Failed to connect to the database.");
	            e.printStackTrace();
	        }

	        return null;
	    }

	
	public static void loadDataToTable(Connection connection, String filePath, String query, int expectedColumns) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement statement = connection.prepareStatement(query)) {

            String line;
            br.readLine(); // Skip header row

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

             // Skip rows with incomplete data
                if (values.length != expectedColumns) {
                    System.err.println("Skipping incomplete row: " + Arrays.toString(values));
                    continue;
                }

                // Bind parameters to the SQL query
                for (int i = 0; i < values.length; i++) {
                	values[i] = values[i].replace("$", "").trim();
                	statement.setString(i + 1, values[i]);
                    //statement.setString(i + 1, values[i].trim());
                }
                statement.addBatch(); // Add to batch for batch processing
            }

            statement.executeBatch();
            System.out.println("Data loaded successfully from: " + filePath);
        } catch (SQLException e) {
            System.err.println("Error loading data from: " + filePath);
            e.printStackTrace();
        }
    }
	
	// HashMap to store transaction data temporarily (hash table for transactions)
    private static Map<Integer, Transaction> transactionHashTable = new HashMap<>();
    
    // Queue to store customer transaction chunks
    private static Queue<Transaction> transactionQueue = new LinkedList<>();
    
    // Disk buffers for loading customer and product data
    private static List<Customer> customerDiskBuffer = new ArrayList<>();
    private static List<Product> productDiskBuffer = new ArrayList<>();
    
    
    public static void main(String[] args) {
    	
    	// Get database connection using user-provided credentials
        try (Connection connection = getConnection()) {
            if (connection != null) {
                System.out.println("Connected to the database. Ready for queries.");

                // File paths
                String customersFile = "customers_data.csv";
                String productsFile = "products_data.csv";
                String transactionFile = "transactions.csv";
                String outputFilePath = "metadata.csv"; // Output file -> metadata

                try {
                    loadDataToTable(connection, customersFile, "INSERT IGNORE INTO customers_data (customer_id, customer_name, gender) VALUES (?, ?, ?)", 3);
                    loadDataToTable(connection, productsFile, "INSERT IGNORE INTO products_data (productID, productName, productPrice, supplierID, supplierName, storeID, storeName) VALUES (?, ?, ?, ?, ?, ?, ?)", 7);
                    loadDataToTable(connection, transactionFile, "INSERT IGNORE INTO transactions (Order_ID, Order_Date, productID, Quantity_Ordered, customer_id, time_id) VALUES (?, ?, ?, ?, ?, ?)", 6);
                } catch (IOException e) {
                    System.err.println("Error loading data from CSV files: " + e.getMessage());
                    e.printStackTrace();
                }
                
                // Creating metadata.csv (combine result of customers_data and products_data)
                try (FileWriter writer = new FileWriter(outputFilePath)) {
                    // Ensure MySQL driver is loaded
                    Class.forName("com.mysql.cj.jdbc.Driver");

                    // Combine data from customer_data and products_data
                    String query = """
                        SELECT DISTINCT
                            c.customer_id, c.customer_name, c.gender, 
                            p.productID, p.productName, p.productPrice, p.supplierID, p.supplierName, p.storeID, p.storeName 
                        FROM 
                            customers_data c 
                        CROSS JOIN 
                            products_data p
                    """;

                    // Execute the query and write results to the file
                    try (Statement statement = connection.createStatement();
                         ResultSet resultSet = statement.executeQuery(query)) {

                        // Write the header to the file
                        writer.append("customer_id,customer_name,gender,productID,productName,productPrice,supplierID,supplierName,storeID,storeName\n");

                        // Write each row to the file
                        while (resultSet.next()) {
                            writer.append(resultSet.getString("customer_id")).append(",")
                                  .append(resultSet.getString("customer_name")).append(",")
                                  .append(resultSet.getString("gender")).append(",")
                                  .append(resultSet.getString("productID")).append(",")
                                  .append(resultSet.getString("productName")).append(",")
                                  .append(resultSet.getString("productPrice")).append(",")
                                  .append(resultSet.getString("supplierID")).append(",")
                                  .append(resultSet.getString("supplierName")).append(",")
                                  .append(resultSet.getString("storeID")).append(",")
                                  .append(resultSet.getString("storeName")).append("\n");
                        }

                        System.out.println("Metadata file created successfully at: " + outputFilePath);
                    }

                } catch (IOException e) {
                    System.err.println("Error writing to file: " + outputFilePath);
                    e.printStackTrace();
                }

                // Perform MESHJOIN operation
                loadTransactionData(connection);
                loadMDPartitions(connection);
                meshJoin(connection);

                // Execute queries for data warehouse analysis
                queriesDW("Query_1", 2019, connection);
                queriesDW("Query_2", 0, connection);
                queriesDW("Query_3", 0, connection);
                queriesDW("Query_4", 0, connection);
                queriesDW("Query_5", 0, connection);
                queriesDW("Query_6", 0, connection);
                queriesDW("Query_7", 0, connection);
                queriesDW("Query_8", 0, connection);
                queriesDW("Query_9", 0, connection);
                queriesDW("Query_10", 0, connection);

            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }  
    }
    
    // Load transaction data in chunks and add to hash table and queue
    private static void loadTransactionData(Connection connection) throws SQLException {
        String query = "SELECT * FROM transactions LIMIT 100";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        
        // Load each transaction into hash table and queue
        while (rs.next()) {
            Transaction txn = new Transaction(
                rs.getInt("Order_ID"), 
                rs.getDate("Order_Date"), 
                rs.getInt("customer_id"), 
                rs.getInt("productID"), 
                rs.getInt("Quantity_Ordered")
            );
            transactionHashTable.put(txn.getOrderId(), txn);
            transactionQueue.add(txn);
        }
    }

    // Load customer and product data partitions into memory (disk buffers)
    private static void loadMDPartitions(Connection connection) throws SQLException {
        // Load customer data partition
        String customerQuery = "SELECT * FROM customers_data LIMIT 100";
        Statement stmt = connection.createStatement();
        ResultSet rsCustomer = stmt.executeQuery(customerQuery);
        
        while (rsCustomer.next()) {
            Customer customer = new Customer(
                rsCustomer.getInt("customer_id"), 
                rsCustomer.getString("customer_name"), 
                rsCustomer.getString("gender")
            );
            customerDiskBuffer.add(customer);
        }

        // Load product data partition
        String productQuery = "SELECT * FROM products_data LIMIT 100";
        ResultSet rsProduct = stmt.executeQuery(productQuery);
        
        while (rsProduct.next()) {
            Product product = new Product(
                rsProduct.getInt("productID"), 
                rsProduct.getString("productName"), 
                rsProduct.getDouble("productPrice"), 
                rsProduct.getInt("supplierID"), 
                rsProduct.getString("supplierName"), 
                rsProduct.getInt("storeID"), 
                rsProduct.getString("storeName")
            );
            productDiskBuffer.add(product);
        }
    }

    // Perform MESHJOIN and generate transformed data, then write to DW
    private static void meshJoin(Connection connection) throws SQLException {
        String insertQuery = """
            INSERT INTO transformed_data 
            (Order_ID, Order_Date, productID, customer_id, customer_name, gender, 
            Quantity_Ordered, productName, productPrice, supplierID, supplierName, storeID, storeName, total_sale) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE Order_ID = Order_ID; -- Prevent duplicate Order_IDs
        """;

        try (PreparedStatement stmt = connection.prepareStatement(insertQuery)) {
            while (!transactionQueue.isEmpty()) {
                Transaction transaction = transactionQueue.poll();

                boolean matchFound = false;
                for (Customer customer : customerDiskBuffer) {
                    for (Product product : productDiskBuffer) {
                        if (transaction.getProductId() == product.getProductId()) {
                            // Match found; process
                            double total_sale = transaction.getQuantityOrdered() * product.getProductPrice();

                            stmt.setInt(1, transaction.getOrderId());
                            stmt.setDate(2, transaction.getOrderDate());
                            stmt.setInt(3, transaction.getProductId());
                            stmt.setInt(4, transaction.getCustomerId());
                            stmt.setString(5, customer.getCustomerName());
                            stmt.setString(6, customer.getGender());
                            stmt.setInt(7, transaction.getQuantityOrdered());
                            stmt.setString(8, product.getProductName());
                            stmt.setDouble(9, product.getProductPrice());
                            stmt.setInt(10, product.getSupplierId());
                            stmt.setString(11, product.getSupplierName());
                            stmt.setInt(12, product.getStoreId());
                            stmt.setString(13, product.getStoreName());
                            stmt.setDouble(14, total_sale);

                            stmt.addBatch();
                            matchFound = true;
                        }
                    }
                }

                if (!matchFound) {
                    System.err.println("No match found for Order_ID: " + transaction.getOrderId());
                }
            }

            stmt.executeBatch();
            System.out.println("Transformed data loaded into the Data Warehouse.");
        }
    }
    
    public static void queriesDW(String queryName, int year, Connection connection) {
        String sqlQuery = null;

        switch (queryName) {
            case "Query_1":
                sqlQuery = """
                        SELECT 
					        MONTH(Order_Date) AS month, 
					        CASE 
					            WHEN DAYOFWEEK(Order_Date) IN (1, 7) THEN 'Weekend'
					            ELSE 'Weekday'
					        END AS day_type, 
					        productName, 
					        SUM(total_sale) AS total_revenue
					    FROM transformed_data 
					    WHERE YEAR(Order_Date) = 2019
					    GROUP BY MONTH(Order_Date), day_type, productName
					    ORDER BY month, day_type, total_revenue DESC
					    LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;

            case "Query_2":
                sqlQuery = """
                    SELECT 
					    current.storeID,
					    current.quarter,
					    current.current_revenue,
					    previous.previous_revenue,
					    ROUND(
					        (current.current_revenue - previous.previous_revenue) / previous.previous_revenue * 100, 2
					    ) AS growth_rate
					FROM (
					    SELECT 
					        storeID,
					        QUARTER(Order_Date) AS quarter,
					        SUM(total_sale) AS current_revenue
					    FROM transformed_data
					    WHERE YEAR(Order_Date) = 2019
					    GROUP BY storeID, QUARTER(Order_Date)
					) AS current
					LEFT JOIN (
					    SELECT 
					        storeID,
					        QUARTER(Order_Date) AS quarter,
					        SUM(total_sale) AS previous_revenue
					    FROM transformed_data
					    WHERE YEAR(Order_Date) = 2019 
					    GROUP BY storeID, QUARTER(Order_Date)
					) AS previous
					ON current.storeID = previous.storeID 
					   AND current.quarter = previous.quarter + 1  -- Compare current quarter with previous quarter
					ORDER BY current.storeID, current.quarter
					LIMIT 5;
                    """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_3":
                sqlQuery = """
                        SELECT 
                            storeID, 
                            storeName, 
                            supplierID, 
                            supplierName, 
                            productName, 
                            SUM(total_sale) AS total_sales
                        FROM transformed_data
                        GROUP BY storeID, storeName, supplierID, supplierName, productName
                        ORDER BY storeID, supplierID, productName
                        LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;

            case "Query_4":
                sqlQuery = """
                        SELECT 
                            productName,
                            CASE 
                                WHEN MONTH(Order_Date) IN (3, 4, 5) THEN 'Spring'
                                WHEN MONTH(Order_Date) IN (6, 7, 8) THEN 'Summer'
                                WHEN MONTH(Order_Date) IN (9, 10, 11) THEN 'Fall'
                                ELSE 'Winter'
                            END AS season,
                            SUM(total_sale) AS total_sales
                        FROM transformed_data
                        GROUP BY productName, season
                        ORDER BY productName, season
                        LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_5":
                sqlQuery = """
                        SELECT
						    t1.storeID,
						    t1.supplierID,
						    MONTH(MIN(t1.Order_Date)) AS month,
						    SUM(t1.total_sale) AS current_revenue,
						    ABS(SUM(t1.total_sale) - IFNULL((
						        SELECT SUM(t2.total_sale)
						        FROM transformed_data t2
						        WHERE t2.storeID = t1.storeID 
						          AND t2.supplierID = t1.supplierID 
						          AND MONTH(t2.Order_Date) = MONTH(MIN(t1.Order_Date)) - 1
						    ), 0)) AS revenue_difference,
						    ROUND(
						        ABS(SUM(t1.total_sale) - IFNULL((
						            SELECT SUM(t2.total_sale)
						            FROM transformed_data t2
						            WHERE t2.storeID = t1.storeID 
						              AND t2.supplierID = t1.supplierID 
						              AND MONTH(t2.Order_Date) = MONTH(MIN(t1.Order_Date)) - 1
						        ), 0)) / GREATEST(IFNULL((
						            SELECT SUM(t2.total_sale)
						            FROM transformed_data t2
						            WHERE t2.storeID = t1.storeID 
						              AND t2.supplierID = t1.supplierID 
						              AND MONTH(t2.Order_Date) = MONTH(MIN(t1.Order_Date)) - 1
						        ), 1), 1) * 100, 2
						    ) AS revenue_volatility
						FROM transformed_data t1
						GROUP BY t1.storeID, t1.supplierID, MONTH(t1.Order_Date)
						ORDER BY t1.storeID, t1.supplierID, month
						LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_6":
                sqlQuery = """
                        SELECT 
						    LEAST(t1.productID, t2.productID) AS product1,
						    GREATEST(t1.productID, t2.productID) AS product2,
						    COUNT(*) AS purchase_count
						FROM transformed_data t1
						JOIN transformed_data t2 
						    ON t1.Order_ID = t2.Order_ID 
						    AND t1.productID < t2.productID
						GROUP BY product1, product2
						ORDER BY purchase_count DESC
						LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_7":
                sqlQuery = """
                        SELECT 
						    storeID,
						    supplierID,
						    productID,
						    YEAR(Order_Date) AS year,
						    SUM(total_sale) AS total_revenue
						FROM transformed_data
						GROUP BY ROLLUP(storeID, supplierID, productID, YEAR(Order_Date))
						ORDER BY storeID, supplierID, productID, year
						LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_8":
                sqlQuery = """
                        SELECT 
						    productID,
						    productName,
						    CASE 
						        WHEN MONTH(Order_Date) <= 6 THEN 'H1'
						        ELSE 'H2'
						    END AS half_year,
						    SUM(total_sale) AS total_revenue,
						    SUM(Quantity_Ordered) AS total_quantity
						FROM transformed_data
						GROUP BY productID, productName, half_year
						ORDER BY productID, half_year
						LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_9":
                sqlQuery = """
                       WITH daily_sales AS (
						    SELECT 
						        productID,
						        productName,
						        Order_Date,
						        SUM(total_sale) AS daily_revenue
						    FROM transformed_data
						    GROUP BY productID, productName, Order_Date
						),
						average_sales AS (
						    SELECT 
						        productID,
						        AVG(daily_revenue) AS avg_daily_revenue
						    FROM daily_sales
						    GROUP BY productID
						)
						SELECT 
						    d.productID,
						    d.productName,
						    d.Order_Date,
						    d.daily_revenue,
						    a.avg_daily_revenue,
						    CASE 
						        WHEN d.daily_revenue > 2 * a.avg_daily_revenue THEN 'Outlier'
						        ELSE 'Normal'
						    END AS revenue_spike_flag
						FROM daily_sales d
						JOIN average_sales a
						    ON d.productID = a.productID
						ORDER BY d.productID, d.Order_Date
						LIMIT 5;
                        """;
                QueryResult(connection, sqlQuery, queryName);
                break;
                
            case "Query_10":
                        String sqlDropView = "DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;";
                        sqlQuery = """

				        CREATE VIEW STORE_QUARTERLY_SALES AS
				        SELECT 
				            storeID,
				            storeName,
				            QUARTER(Order_Date) AS quarter,
				            SUM(total_sale) AS total_quarterly_sales
				        FROM transformed_data
				        GROUP BY storeID, storeName, QUARTER(Order_Date)
				        ORDER BY storeName, quarter
						LIMIT 5;
                        """;
                        QueryResult(connection, sqlQuery, queryName);
                
                		try (Statement stmt = connection.createStatement()) {
                	        // Drop the view if it exists
                	        stmt.executeUpdate(sqlDropView);
                	        // Create the new view
                	        stmt.executeUpdate(sqlQuery);
                    System.out.println("View STORE_QUARTERLY_SALES created successfully.");
                } catch (SQLException e) {
                    System.err.println("Error creating view STORE_QUARTERLY_SALES.");
                    e.printStackTrace();
                }
                
                break;

            default:
                System.err.println("Invalid query name: " + queryName);
                return;
        }
    }
        
        public static void QueryResult(Connection connection, String sqlQuery, String queryName) {
            if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
                System.err.println("Query string is null or empty. Cannot execute.");
                return;
            }

            try (Statement stmt = connection.createStatement()) {
                // Determine if the query is a DDL (CREATE, DROP, ALTER) or DML (SELECT)
                if (sqlQuery.trim().startsWith("CREATE") || sqlQuery.trim().startsWith("DROP") || sqlQuery.trim().startsWith("ALTER")) {
                	stmt.executeUpdate("DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;");
                    // Handle DDL queries
                    System.out.println(queryName +" Executed Successfully.");
                } else {
                    // Handle DML queries (SELECT)
                	try (ResultSet rs = stmt.executeQuery(sqlQuery)) {
                        System.out.printf("Executing query: %s%n", queryName);
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();

                        // Print column headers
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.printf("%-20s", metaData.getColumnName(i));
                        }
                        System.out.println();
                        System.out.println("-".repeat(columnCount * 20));

                        // Print result rows
                        while (rs.next()) {
                            for (int i = 1; i <= columnCount; i++) {
                                System.out.printf("%-20s", rs.getString(i));
                            }
                            System.out.println();
                        }
                    }
                }
              }
                catch (SQLException e) {
            	System.err.println("Error executing query: " + queryName);
                e.printStackTrace();
            }
    }
    

    // Helper classes
    static class Transaction {
        private int Order_ID;
        private Date Order_Date;
        private int customer_id;
        private int productID;
        private int Quantity_Ordered;
        private double total_sale;

        public Transaction(int Order_ID, Date Order_Date, int customer_id, int productID, int Quantity_Ordered) {
            this.Order_ID = Order_ID;
            this.Order_Date = Order_Date;
            this.customer_id = customer_id;
            this.productID = productID;
            this.Quantity_Ordered = Quantity_Ordered;
        }

        public int getOrderId() {
            return Order_ID;
        }

        public Date getOrderDate() {
            return Order_Date;
        }

        public int getCustomerId() {
            return customer_id;
        }

        public int getProductId() {
            return productID;
        }

        public int getQuantityOrdered() {
            return Quantity_Ordered;
        }

        public void setTotalSale(double total_sale) {
            this.total_sale = total_sale;
        }

        @Override
        public String toString() {
            return "Transaction{" + "Order_ID=" + Order_ID + "Order_Date=" + Order_Date + ", totalSale=" + total_sale + '}';
        }
    }

    static class Customer {
        private int customer_id;
        private String customer_name;
        private String gender;

        public Customer(int customer_id, String customer_name, String gender) {
            this.customer_id = customer_id;
            this.customer_name = customer_name;
            this.gender = gender;
        }

        public int getCustomerId() {
            return customer_id;
        }

        public String getCustomerName() {
            return customer_name;
        }

        public String getGender() {
            return gender;
        }
    }

    static class Product {
        private int productID;
        private String productName;
        private double productPrice;
        private int supplierID;
        private String supplierName;
        private int storeID;
        private String storeName;

        public Product(int productID, String productName, double productPrice, int supplierID, String supplierName, int storeID, String storeName) {
            this.productID = productID;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierID = supplierID;
            this.supplierName = supplierName;
            this.storeID = storeID;
            this.storeName = storeName;
        }

        public int getProductId() {
            return productID;
        }

        public String getProductName() {
            return productName;
        }

        public double getProductPrice() {
            return productPrice;
        }

        public int getSupplierId() {
            return supplierID;
        }

        public String getSupplierName() {
            return supplierName;
        }

        public int getStoreId() {
            return storeID;
        }

        public String getStoreName() {
            return storeName;
        }
    }
    
}

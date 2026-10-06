import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day13SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13-Spark-SQL-Basics")
      .master("local[*]")
      .getOrCreate()

    println("\n========== DAY 13: SPARK SQL BASICS ==========")

    // =========================================================
    // 1. READ CSV
    // =========================================================

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\n========== CUSTOMER DATA ==========")
    customers.show()

    // =========================================================
    // 2. INSPECT SCHEMA
    // =========================================================

    println("\n========== SCHEMA ==========")
    customers.printSchema()

    // =========================================================
    // 3. SELECT COLUMNS
    // =========================================================

    println("\n========== SELECT COLUMNS ==========")

    customers
      .select("customer_id", "name", "city")
      .show()

    // =========================================================
    // 4. FILTER
    // =========================================================

    println("\n========== HYDERABAD CUSTOMERS ==========")

    customers
      .filter(col("city") === "Hyderabad")
      .show()

    // =========================================================
    // 5. FILTER USING SQL EXPRESSION
    // =========================================================

    println("\n========== CUSTOMERS WITH SPENDING > 50000 ==========")

    customers
      .filter("total_spent > 50000")
      .show()

    // =========================================================
    // 6. WITHCOLUMN
    // =========================================================

    println("\n========== WITHCOLUMN ==========")

    val updatedCustomers = customers
      .withColumn(
        "spending_level",
        when(col("total_spent") >= 70000, "High")
          .when(col("total_spent") >= 40000, "Medium")
          .otherwise("Low")
      )

    updatedCustomers.show()

    // =========================================================
    // 7. CREATE TEMPORARY VIEW
    // =========================================================

    updatedCustomers.createOrReplaceTempView("customers")

    println("\n========== TEMPORARY VIEW CREATED ==========")

    // =========================================================
    // 8. SPARK SQL
    // =========================================================

    println("\n========== SPARK SQL: ALL CUSTOMERS ==========")

    spark.sql("""
      SELECT *
      FROM customers
    """).show()

    // =========================================================
    // 9. SPARK SQL FILTER
    // =========================================================

    println("\n========== SPARK SQL: HYDERABAD CUSTOMERS ==========")

    spark.sql("""
      SELECT customer_id, name, city, total_spent
      FROM customers
      WHERE city = 'Hyderabad'
    """).show()

    // =========================================================
    // 10. SPARK SQL ORDER BY
    // =========================================================

    println("\n========== TOP CUSTOMERS BY SPENDING ==========")

    spark.sql("""
      SELECT customer_id, name, city, total_spent
      FROM customers
      ORDER BY total_spent DESC
    """).show()

    // =========================================================
    // 11. CUSTOMER ANALYTICS REPORT
    // =========================================================

    println("\n========== CUSTOMER ANALYTICS REPORT ==========")

    val report = spark.sql("""
      SELECT
        city,
        COUNT(*) AS customer_count,
        ROUND(AVG(total_spent), 2) AS average_spending,
        MAX(total_spent) AS highest_spending,
        MIN(total_spent) AS lowest_spending
      FROM customers
      GROUP BY city
      ORDER BY average_spending DESC
    """)

    report.show()

    // =========================================================
    // 12. READ JSON
    // =========================================================

    println("\n========== JSON DATA ==========")

    val jsonCustomers = spark.read
      .option("inferSchema", "true")
      .json("data/customers.json")

    jsonCustomers.show()

    println("\n========== JSON SCHEMA ==========")
    jsonCustomers.printSchema()

    spark.stop()
  }
}

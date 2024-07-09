import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties

object Consumer extends SparkSessionTrait {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    SparkSessionTrait("Consumer")

    try {
      // Define the directory from which to read files
      val inputDir = "outputs"

      // Infinite loop to read files every 10 seconds
      while (true) {
        // Read all CSV files from the input directory
        val df = spark.read.option("header", true).csv(inputDir + "/*")

        // Perform any necessary processing
        val processedDF = process(df)

        // Example: Print schema and show some rows
        println(s"Processing batch at: ${java.time.LocalDateTime.now()}")
        processedDF.printSchema()
        processedDF.show()

        // Write processed data to MySQL
        writeToMySQL(processedDF)

        // Wait for 10 seconds before reading files again
        Thread.sleep(1000)
      }

    } finally {
      spark.stop()
    }
  }


  // Function to write data to MySQL with timestamp
  def writeToMySQL(df: DataFrame): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/spark_streaming_db"
    val tableName = "gender_counts"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "12345678")

    // Add a timestamp column to the DataFrame before writing to MySQL
    val dfWithTimestamp = df.withColumn("timestamp_column", current_timestamp())

    // Write to MySQL
    dfWithTimestamp.write.mode("overwrite")
      .jdbc(jdbcUrl, tableName, connectionProperties)
  }

  // Example processing function
  def process(df: DataFrame): DataFrame = {
    // Perform transformations or computations on df
    // Example: Calculate count of records for each Gender
    df.groupBy(col("Gender")).count()
  }
}

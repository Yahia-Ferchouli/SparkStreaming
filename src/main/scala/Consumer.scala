import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.{Connection, DriverManager, PreparedStatement}

object Consumer extends SparkSessionTrait {

  // Centralized JDBC connection parameters
  private val jdbcUrl = "jdbc:mysql://localhost:3306/spark_streaming_db"
  private val username = "root"
  private val password = "12345678"

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    SparkSessionTrait("Consumer")

    try {
      // Define the directory from which to read files
      val inputDir = "outputs"

      // Define the schema of the CSV files
      val schema = new StructType()
        .add("id", IntegerType, true)
        .add("Gender", StringType, true)
        .add("Customer Type", StringType, true)
        .add("Age", IntegerType, true)
        .add("Type of Travel", StringType, true)
        .add("Class", StringType, true)
        .add("Flight Distance", IntegerType, true)
        .add("Inflight wifi service", IntegerType, true)
        .add("Departure/Arrival time convenient", IntegerType, true)
        .add("Ease of Online booking", IntegerType, true)
        .add("Gate location", IntegerType, true)
        .add("Food and drink", IntegerType, true)
        .add("Online boarding", IntegerType, true)
        .add("Seat comfort", IntegerType, true)
        .add("Inflight entertainment", IntegerType, true)
        .add("On-board service", IntegerType, true)
        .add("Leg room service", IntegerType, true)
        .add("Baggage handling", IntegerType, true)
        .add("Checkin service", IntegerType, true)
        .add("Inflight service", IntegerType, true)
        .add("Cleanliness", IntegerType, true)
        .add("Departure Delay in Minutes", IntegerType, true)
        .add("Arrival Delay in Minutes", DoubleType, true)
        .add("satisfaction", StringType, true)

      // Read all CSV files from the input directory as a streaming source
      val df = spark.readStream
        .option("header", true)
        .schema(schema)
        .csv(inputDir + "/*")

      // Perform processing to get both KPIs
      val (genderCountDF, satisfactionByClassDF) = process(df)

      // Start the streaming query and await termination
      val query = genderCountDF.writeStream
        .outputMode("update") // Change to "update" or "complete" as needed
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          kpi1(batchDF)
        }
        .start()

      val query2 = satisfactionByClassDF.writeStream
        .outputMode("update") // Change to "update" or "complete" as needed
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          kpi2(batchDF)
        }
        .start()

      query.awaitTermination()
      query2.awaitTermination()

    } finally {
      spark.stop()
    }
  }

  // Function to process the data and return DataFrames for both KPIs
  def process(df: DataFrame): (DataFrame, DataFrame) = {
    // KPI 1: Count of records for each Gender
    val genderCountDF = df.groupBy(col("Gender")).count()

    // KPI 2: Count of records for each Class and satisfaction
    val satisfactionByClassDF = df.groupBy(col("Class"), col("satisfaction")).count()

    (genderCountDF, satisfactionByClassDF)
  }

  // Function to handle insertion into the gender_counts table
  def kpi1(batchDF: DataFrame): Unit = {
    // Connect to the database
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO gender_counts (Gender, Count)
            |VALUES (?, ?)
            |ON DUPLICATE KEY UPDATE
            |Count = VALUES(Count)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setString(1, row.getAs[String]("Gender"))
            upsertStatement.setLong(2, row.getAs[Long]("count"))
            upsertStatement.executeUpdate()
          }
        } finally {
          upsertStatement.close()
        }
      } finally {
        conn.close()
      }
    }
  }

  // Function to handle insertion into the satisfaction_by_class table
  def kpi2(batchDF: DataFrame): Unit = {
    // Connect to the database
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO satisfaction_by_class (class, satisfaction, count)
            |VALUES (?, ?, ?)
            |ON DUPLICATE KEY UPDATE
            |count = VALUES(count)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setString(1, row.getAs[String]("Class"))
            upsertStatement.setString(2, row.getAs[String]("satisfaction"))
            upsertStatement.setLong(3, row.getAs[Long]("count"))
            upsertStatement.executeUpdate()
          }
        } finally {
          upsertStatement.close()
        }
      } finally {
        conn.close()
      }
    }
  }
}

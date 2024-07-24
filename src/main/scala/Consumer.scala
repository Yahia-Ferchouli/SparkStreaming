import org.apache.spark.sql.{SparkSession, DataFrame, Row, SaveMode}
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
      val inputDir = s"$outputPath"

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
      val (genderCountDF, satisfactionByClassDF, typeTravelCountDF, ageDistDF, loyalCustomerCountByAgeDF) = process(df)
//      val satisfactionByFeatureDF = satisfaction_by_feature_process(df)

//      println(loyalCustomerCount.show(10, truncate = false))
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

      val query3 = typeTravelCountDF.writeStream
        .outputMode("update") // Change to "update" or "complete" as needed
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          kpi3(batchDF)
        }
        .start()

      val query4 = ageDistDF.writeStream
        .outputMode("update")
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        kpi4(batchDF)
        }
        .start()

      val query5 = loyalCustomerCountByAgeDF.writeStream
        .outputMode("update")
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          kpiLoyalCustomerCount(batchDF)
        }
        .start()

      query.awaitTermination()
      query2.awaitTermination()
      query3.awaitTermination()
      query4.awaitTermination()

    } finally {
      spark.stop()
    }
  }


  val satisfactionToNumeric = udf((satisfaction: String) => satisfaction match {
    case "satisfied" => 1
    case _ => 0
  })

  val loyalToNumeric = udf((loyal: String) => loyal match {
    case "Loyal Customer" => 1
    case _ => 0
  })

  // Function to process the data and return DataFrames for both KPIs
  def process(df: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    // KPI 1: Count of records for each Gender
    val genderCountDF = df.groupBy(col("Gender")).count()

    val satisfactionCountDF = df.withColumn("satisfactionNumeric", satisfactionToNumeric(col("satisfaction")))
      .groupBy(col("satisfactionNumeric"))
      .count()

    val satisfactionByClassDF = df.groupBy(col("Class"), col("satisfaction")).count()

    val typeTravelCountDF = df.groupBy(col("Type of Travel")).count()

    val ageDistDF = df.groupBy(col("Age")).count()

    val loyalCustomerCountByAgeDF = df
      .withColumn("LoyalNumeric", loyalToNumeric(col("Customer Type")))
      .groupBy(col("Age"))
      .agg(
        sum("LoyalNumeric").alias("Loyal Customer Count"),
       ( count("Customer Type") - sum("LoyalNumeric")).alias("Disloyal Customer Count")
      )

//    (genderCountDF, satisfactionByClassDF, impactOfFlightDistanceDF, loyalCustomerCountByAgeDF, ageDistDF, satisfactionByTravelTypeDF)
    (genderCountDF, satisfactionCountDF, typeTravelCountDF, ageDistDF, loyalCustomerCountByAgeDF)
  }

  // Function to handle insertion into the gender_counts table
  def kpi1(batchDF: DataFrame): Unit = {
    // Connect to the database
    batchDF.show(5)
    batchDF.printSchema()
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
    batchDF.show(5)
    batchDF.printSchema()
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

  def kpi3(batchDF: DataFrame): Unit = {
    // Connect to the database
//    batchDF.show(5)
//    batchDF.printSchema()
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO type_travel_counts (Type_of_Travel, Count)
            |VALUES (?, ?)
            |ON DUPLICATE KEY UPDATE
            |Count = VALUES(Count)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setString(1, row.getAs[String]("Type of Travel"))
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

  def kpi4(batchDF: DataFrame):Unit = {
//    batchDF.show(5)
//    batchDF.printSchema()
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO age_distribution (Age, Count)
            |VALUES (?, ?)
            |ON DUPLICATE KEY UPDATE
            |Count = VALUES(Count)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setInt(1, row.getAs[Int]("Age"))
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

  def kpiImpactOfFlightDistance(batchDF: DataFrame): Unit = {
    //Connect to the database
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO flight_distance_impact (Flight Distance, Average Satisfaction)
            |VALUES (?, ?)
            |ON DUPLICATE KEY UPDATE
            |Average Satisfaction = VALUES(Average Satisfaction)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setInt(1, row.getAs[Int]("Flight Distance"))
            upsertStatement.setDouble(2, row.getAs[Double]("Average Satisfaction"))
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

  def kpiLoyalCustomerCount(batchDF: DataFrame): Unit = {
    batchDF.printSchema()
    batchDF.show(5)
    //Connect to the database
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO loyalty_by_age (Age, LoyalCount, DisloyalCount)
            |VALUES (?, ?, ?)
            |ON DUPLICATE KEY UPDATE
            |LoyalCount = VALUES(LoyalCount),
            |DisloyalCount = VALUES(DisloyalCount)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setInt(1, row.getAs[Int]("Age"))
            upsertStatement.setInt(2, row.getAs[Int]("Loyal Customer Count"))
            upsertStatement.setInt(3, row.getAs[Int]("Disloyal Customer Count"))
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

//  def satisfaction_by_feature_process(df: DataFrame): DataFrame = {
//    val dfWithBinarySatisfaction = df.withColumn("binary_satisfaction", when(col("satisfaction") === "satisfied", 1).otherwise(0))
//
//    val features = Seq(
//      "Inflight wifi service",
//      "Departure/Arrival time convenient",
//      "Ease of Online booking",
//      "Gate location",
//      "Food and drink",
//      "Online boarding",
//      "Seat comfort",
//      "Inflight entertainment",
//      "On-board service",
//      "Leg room service",
//      "Baggage handling",
//      "Checkin service",
//      "Inflight service",
//      "Cleanliness"
//    )
//
//    // Initialize an empty DataFrame for the results
//    var resultDF = spark.emptyDataFrame
//
//    // Calculate the mean binary satisfaction for each feature and concatenate the results
//    for (feature <- features) {
//      val featureDF = dfWithBinarySatisfaction.groupBy(col(feature).as("feature_value"))
//        .agg(avg("binary_satisfaction").cast("decimal(2,2)").as("mean_satisfaction"))
//        .withColumn("feature_name", lit(feature))
//
//      resultDF = if (resultDF.isEmpty) featureDF else resultDF.union(featureDF)
//    }
//
//    resultDF
//  }

  def kpi_satisfaction_by_feature_process(batchDF: DataFrame): Unit = {
    // Connect to the database
    batchDF.foreachPartition { partition: Iterator[Row] =>
      val conn: Connection = DriverManager.getConnection(jdbcUrl, username, password)
      try {
        val upsertStatement: PreparedStatement = conn.prepareStatement(
          """
            |INSERT INTO mean_satisfaction_by_feature (feature_name, feature_value, mean_satisfaction)
            |VALUES (?, ?, ?)
            |ON DUPLICATE KEY UPDATE
            |mean_satisfaction = VALUES(mean_satisfaction)
            """.stripMargin
        )
        try {
          // Process each row in the partition
          partition.foreach { row =>
            upsertStatement.setString(1, row.getAs[String]("feature_name"))
            upsertStatement.setInt(2, row.getAs[Int]("feature_value"))
            upsertStatement.setDouble(3, row.getAs[Double]("mean_satisfaction"))
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

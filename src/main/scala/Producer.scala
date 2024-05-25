



import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Producer {

  def main(args: Array[String]): Unit = {
    val inputFile = "src/dataset/train.csv"
    val linesPerSegment = 10000
    val outputPath = "outputs"

    val spark = SparkSession.builder()
      .appName("Spark File Splitter")
      .master("local[*]")
      .getOrCreate()

    try {
      val inputDF = spark.read.text(inputFile)
      val segmentedDFs = splitFile(inputDF, linesPerSegment, outputPath)
    } finally {
      spark.stop()
    }
  }

  def splitFile(inputDF: DataFrame, linesPerSegment: Int, outputPath: String): Array[DataFrame] = {
    // Count the total number of rows in the DataFrame
    val totalRows = inputDF.count()

    // Calculate the total number of segments
    val totalSegments = Math.ceil(totalRows.toDouble / linesPerSegment).toInt

    // Create a DataFrame for the header
    val headerDF = inputDF.limit(0)

    // Create a DataFrame for each segment
    val segmentedDFs = (0 until totalSegments).map { segmentIndex =>
      // Calculate the starting and ending row numbers for the current segment
      val startRow = segmentIndex * linesPerSegment
      val endRow = Math.min((segmentIndex + 1) * linesPerSegment, totalRows).toInt

      // Filter the data for the current segment
      val segmentDF = inputDF.limit(endRow).drop("row_number")

      // Concatenate the header with the segment data
      val segmentWithHeaderDF = headerDF.union(segmentDF)

      // Write the segment to a CSV file
      segmentWithHeaderDF.write.mode("overwrite").option("header", true).csv(s"$outputPath/segment_$segmentIndex")

      // Return the DataFrame of the segment
      segmentWithHeaderDF
    }

    segmentedDFs.toArray
  }


}





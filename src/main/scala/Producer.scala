import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Producer extends SparkSessionTrait {

  def main(args: Array[String]): Unit = {

    SparkSessionTrait("Producer")

    try {
      val inputDF = spark.read.option("header", true).csv(inputFile)
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

    // Create a DataFrame for each segment
    val segmentedDFs = (0 until totalSegments).map { segmentIndex =>
      // Calculate the starting and ending row numbers for the current segment
      val startRow = if (segmentIndex != 0) {
        segmentIndex * linesPerSegment + 1
      } else {
        segmentIndex * linesPerSegment
      }

      val endRow = Math.min((segmentIndex + 1) * linesPerSegment, totalRows).toInt

      // Filter the data for the current segment
      val segmentDF = inputDF.filter(col("_c0").between(startRow, endRow))

      // Delete index column
      val cleanedSegmentDF = segmentDF.drop("_c0")

      // Write the segment to a CSV file
      cleanedSegmentDF.write.mode("overwrite").option("header", true).csv(s"$outputPath/segment_$segmentIndex")

      // Return the DataFrame of the segment
      cleanedSegmentDF
    }
    segmentedDFs.toArray
  }


}





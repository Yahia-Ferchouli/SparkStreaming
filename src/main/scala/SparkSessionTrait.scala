import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {

  def createSparkConf: SparkConf = new SparkConf()

  lazy val inputFile = "src/dataset/train.csv"
  lazy val linesPerSegment = 10000
  lazy val outputPath = "outputs"

  lazy val spark: SparkSession = {
    try {
      SparkSession.builder()
        .config(createSparkConf)
        .getOrCreate()
    } catch {
      case e: Exception =>
        throw new Exception("Spark session could not be created" + e.getMessage)
    }
  }

  object SparkSessionTrait {
    def apply(iAppName: String,
              iMaster: String = "local[*]"
             ): SparkSessionTrait = {
      val instance = new SparkSessionTrait {
        override def createSparkConf: SparkConf = {
          new SparkConf()
            .setAppName(iAppName)
            .setMaster(iMaster)
        }
      }
      instance.inputFile
      instance.linesPerSegment
      instance.outputPath
      instance.spark
      instance
    }
  }
}
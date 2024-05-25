import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{BufferedOutputStream, File, FileOutputStream}
import scala.io.Source

object Helper extends Serializable {
  def removePunctuation(text: String): String = {
    val punctPattern = "[^a-zA-Z0-9\\s]".r
    punctPattern.replaceAllIn(text, "").toLowerCase
  }
}

object FileReconstitution {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Reconstitution")
    val ssc = new StreamingContext(conf, Seconds(1))

    val segment_dir = "outputs"

    // Define a method to extract the numerical part of the segment filenames
    def extractSegmentNumber(filename: String): Int = {
      val SegmentPattern = """segment_(\d+)""".r
      filename match {
        case SegmentPattern(number) => number.toInt
        case _ => Int.MaxValue // In case the filename does not match the pattern
      }
    }

    // traiter les segments du dossier output
    val output_file = new File("harry_potter_1_reconstitution")
    val output_stream = new BufferedOutputStream(new FileOutputStream(output_file, true)) // Append mode

    val existingFiles = new File(segment_dir).listFiles()
      .filter(_.isFile)
      .sortBy(file => extractSegmentNumber(file.getName))

    println(existingFiles.mkString("Array(", ", ", ")"))


    // serialisation pour éviter les erreurs
    for (file <- existingFiles) {
      val source = Source.fromFile(file)
      for (line <- source.getLines()) {
        val cleanSegment = Helper.removePunctuation(line)
        output_stream.write(cleanSegment.getBytes)
      }
      source.close()
    }
    output_stream.close()

    // lire les segments sous forme de stream text
    val lines = ssc.textFileStream(segment_dir)

    lines.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // ouvrir le fichier output
        val output_stream = new BufferedOutputStream(new FileOutputStream(output_file, true)) // Append mode

        // écrire chaque segment dans la partition dans le fichier output après nettoyage
        partition.foreach { segment =>
          val cleanSegment = Helper.removePunctuation(segment)
          output_stream.write(cleanSegment.getBytes)
        }

        // fermer l'output stream pour la partition
        output_stream.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

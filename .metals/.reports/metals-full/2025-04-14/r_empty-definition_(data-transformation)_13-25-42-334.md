error id: scala/collection/SeqLike#size().
file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol scala/collection/SeqLike#size().
empty definition using fallback
non-local guesses:

offset: 2376
uri: file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object ParquetReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Parquet Reader")
      .master("local[*]")
      .getOrCreate()

    try {
      val parquetFilePath_master = "C:/Users/josephlim/Downloads/file2.parquet"
      val parquetFilePath_transaction = "C:/Users/josephlim/Downloads/sample_data_with_duplicates.parquet"
      
      // Read and display master data
      val rdd_master = readParquetFile(spark, parquetFilePath_master)
      val newRecord = "2,a Bluff"
      val rdd_master_with_new_record = spark.sparkContext.parallelize(Seq(newRecord)).union(rdd_master)

      val deduped_rdd_master = dedupeRDD(rdd_master_with_new_record, Seq(1))
      showTopRecords(deduped_rdd_master, 10)  // Display top 10 records

      // Read and display transaction data
      //val rdd_trnx = readParquetFile(spark, parquetFilePath_transaction)
      //showTopRecords(rdd_trnx, 10)  // Display top 10 records
    } catch {
      case e: java.io.FileNotFoundException => println(s"File not found: ${e.getMessage}")
      case e: org.apache.spark.sql.AnalysisException => println(s"Error reading Parquet file: ${e.getMessage}")
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  // Function to read Parquet file and convert to RDD
  def readParquetFile(spark: SparkSession, filePath: String): RDD[String] = {
    val df = spark.read.parquet(filePath)
    df.rdd.map(row => row.mkString(","))
  }

  // Function to display top records from RDD
  def showTopRecords(rdd: RDD[String], numRecords: Int): Unit = {
    rdd.take(numRecords).foreach(println)
  }

// Function to deduplicate RDD based on key column indices without sorting
def dedupeRDD(rdd: RDD[String], keyIndices: Seq[Int]): RDD[String] = {
  rdd.mapPartitionsWithIndex { (idx, iter) =>
    if (idx == 0) iter.drop(1) else iter
  }.map { row =>
    val columns = row.split(",")
    val key = keyIndices.map(columns(_)).mkString(",")
    (key, columns)
  }.groupByKey().map { case (key, records) =>
    val recordList = records.toList
    val distinctRecords = recordList.distinct
    val latestRecord = recordList.last
    val validFlag = if (recordList.size@@ == 1) {
      "Y"
    } else if (distinctRecords.size == 1) {
      "X"
    } else {
      "N"
    }
    latestRecord.mkString(",") + s",$validFlag"
  }
}





}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
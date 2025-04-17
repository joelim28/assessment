error id: local6
file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol local6
empty definition using fallback
non-local guesses:

offset: 864
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
      val deduped_rdd_master = dedupeRDD(rdd_master, Seq(0))
      //showTopRecords(deduped_rdd_master, 11)  // Display top 11 records

      // Read and display transaction data
      val rdd_trnx = readParquetFile(spark, parquetFilePath_transaction)
      val deduped_rdd_@@trnx = dedupeRDDFirst(rdd_trnx, Seq(2), Seq(4))
      
      // Filter and display records based on column index and filter value
      //val filteredRecords = filterRecords(deduped_rdd_trnx, 2, "28995")
      //showTopRecords(filteredRecords, 10)  // Display top 10 filtered records

      
      // Perform left join operation
      val leftJoinedRDD = leftJoinRDDs(rdd1, rdd2, 0, 0)


      showTopRecords(deduped_rdd_trnx, 10)  // Display top 10 records
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

  // Function to filter records based on column index and filter value
  def filterRecords(rdd: RDD[String], columnIndex: Int, filterValue: String): RDD[String] = {
    rdd.filter(record => record.split(",")(columnIndex) == filterValue)
  }
  
  // Function to display top records from RDD
  def showTopRecords(rdd: RDD[String], numRecords: Int): Unit = {
    rdd.take(numRecords).foreach(println)
  }

  // Function to deduplicate RDD based on key column indices without sorting
   //Valid Flag Scenario = "X" where record is duplicated as identical based on key column, "N" where record is duplicated as non-identical based on key column, "Y" is the valid record based on first occurrence
  def dedupeRDD(rdd: RDD[String], keyIndices: Seq[Int]): RDD[String] = {
    rdd.map(record => {
      val key = keyIndices.map(record.split(",")(_)).mkString(",")
      (key, record)
    })
    .groupByKey()
    .flatMap { case (key, records) =>
      val sortedRecords = records.toList.sortBy(identity)
      sortedRecords.zipWithIndex.map { case (record, index) =>
        val validFlag = if (index == sortedRecords.size - 1) "Y" else if (sortedRecords.count(_ == record) > 1) "X" else "N"
        record + "," + validFlag
      }
    }
  }

  // Function to deduplicate RDD based on key column indices, keeping the first occurrence based on sorting sortColumnIndices ascending 
  //Valid Flag Scenario = "X" where record is duplicated as identical based on key column, "N" where record is duplicated as non-identical based on key column, "Y" is the valid record based on first occurrence
 def dedupeRDDFirst(rdd: RDD[String], keyIndices: Seq[Int], sortColumnIndices: Seq[Int]): RDD[String] = {
    rdd.map(record => {
      val key = keyIndices.map(record.split(",")(_)).mkString(",")
      (key, record)
    })
    .groupByKey()
    .flatMap { case (key, records) =>
      // Sort records based on the specified sort column indices
      val sortedRecords = records.toList.sortBy(record => sortColumnIndices.map(record.split(",")(_)).mkString(","))
      sortedRecords.zipWithIndex.map { case (record, index) =>
        val validFlag = if (index == 0) "Y" else if (sortedRecords.count(_ == record) > 1) "X" else "N"
        record + "," + validFlag
      }
    }
  }

//Perform a left join of 2 RDD with rdd1 as left and rdd 2 as right.  keyIndex1 and keyIndex2 are the linking key columns correspondingly.
def leftJoinRDDs(rdd1: RDD[String], rdd2: RDD[String], keyIndex1: Int, keyIndex2: Int): RDD[(String, (String, Option[String]))] = {
    // Map each RDD to key-value pairs based on the specified key columns
    val rdd1Keyed = rdd1.map(record => {
      val columns = record.split(",")
      (columns(keyIndex1), record)  // Using the specified column as the key
    })

    val rdd2Keyed = rdd2.map(record => {
      val columns = record.split(",")
      (columns(keyIndex2), record)  // Using the specified column as the key
    })

    // Perform left outer join operation
    rdd1Keyed.leftOuterJoin(rdd2Keyed)
  }

}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
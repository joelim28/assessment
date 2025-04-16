error id: org/apache/spark/sql/
file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 27
uri: file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
text:
```scala
import org.apache.spark.sql@@.SparkSession
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
      val (rdd_master, masterColumnNames) = readParquetFile(spark, parquetFilePath_master)
      /*
      val deduped_rdd_master = dedupeRDD(rdd_master, Seq(0))
      val rdd_master_cleaned = filterRecords(deduped_rdd_master, 2, "Y")
      showTopRecords(rdd_master_cleaned, 11)  // Display top 11 records
      */
      /* Read and display transaction data
      val rdd_trnx = readParquetFile(spark, parquetFilePath_transaction)
      val deduped_rdd_trnx = dedupeRDDFirst(rdd_trnx, Seq(2), Seq(4))
      val rdd_trnx_cleaned = filterRecords(deduped_rdd_trnx,5, "Y")
      
      // Filter and display records based on column index and filter value
      //val filteredRecords = filterRecords(deduped_rdd_trnx, 2, "28995")
      //showTopRecords(filteredRecords, 10)  // Display top 10 filtered records


      // Perform left join operation
      val rdd_ods_stg1 = leftJoinRDDs(rdd_trnx_cleaned, rdd_master_cleaned, 0, 0)

      //showTopRecords(rdd_ods_stg1, 10)  // Display top 10 records

      val rdd_ods = keepColumns(rdd_ods_stg1, Seq(0,1,2,3,4,5,7))
      //showTopRecords(rdd_ods, 10)  // Display top 10 records
      showTopRecords(rdd_ods, 10)


      val rdd_item_summary = countRecordsByColumn(rdd_ods, Seq(0,3))
      showTopRecords(rdd_item_summary, 10)

      val rdd_item_rank_item = addRankColumn(rdd_item_summary, Seq(1), ascending = false)
      showTopRecords(rdd_item_rank_item, 10)
*/
    } catch {
      case e: java.io.FileNotFoundException => println(s"File not found: ${e.getMessage}")
      case e: org.apache.spark.sql.AnalysisException => println(s"Error reading Parquet file: ${e.getMessage}")
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  // Function to read Parquet file and convert to RDD
  // Function to read Parquet file and convert to RDD, returning column names as well
  def readParquetFile(spark: SparkSession, filePath: String): (RDD[String], Seq[String]) = {
    val df = spark.read.parquet(filePath)
    val columnNames = df.columns.toSeq
    val rdd = df.rdd.map(row => row.mkString(","))
    (rdd, columnNames)
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
  def dedupeRDDFirst(rdd: RDD[String], keyIndices: Seq[Int], sortColumnIndices: Seq[Int]): RDD[String] = {
    rdd.map(record => {
      val key = keyIndices.map(record.split(",")(_)).mkString(",")
      (key, record)
    })
    .groupByKey()
    .flatMap { case (key, records) =>
      val sortedRecords = records.toList.sortBy(record => sortColumnIndices.map(record.split(",")(_)).mkString(","))
      sortedRecords.zipWithIndex.map { case (record, index) =>
        val validFlag = if (index == 0) "Y" else if (sortedRecords.count(_ == record) > 1) "X" else "N"
        record + "," + validFlag
      }
    }
  }

  // Function to perform a left join of 2 RDDs with rdd1 as left and rdd2 as right. keyIndex1 and keyIndex2 are the linking key columns correspondingly.
  def leftJoinRDDs(rdd1: RDD[String], rdd2: RDD[String], keyIndex1: Int, keyIndex2: Int): RDD[String] = {
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
    val joinedRDD = rdd1Keyed.leftOuterJoin(rdd2Keyed)

    // Convert the joined RDD to a string RDD with replacements for null values
    joinedRDD.map {
      case (key, (record1, Some(record2))) => s"$record1,$record2"
      case (key, (record1, None)) =>
        val columns = record1.split(",")
        val replacedValue = if (columns(0).forall(_.isDigit)) "-1" else "not found"
        s"$record1,$replacedValue"
    }
  }

  def keepColumns(rdd: RDD[String], columnIndices: Seq[Int]): RDD[String] = {
    rdd.map(record => {
      val columns = record.split(",")
      columnIndices.map(columns(_)).mkString(",")
    })
  }

  def countRecordsByColumn(rdd: RDD[String], keyIndices: Seq[Int]): RDD[String] = {
    rdd.map(record => {
      val key = keyIndices.map(record.split(",")(_)).mkString(",")
      (key, 1)
    }).reduceByKey(_ + _)
      .map { case (key, count) => s"$key,$count" }
  }


  def addRankColumn(
  rdd: RDD[String], 
  columns: Seq[Int], 
  ascending: Boolean
): RDD[String] = {
  // Parse the input RDD to extract columns
  val parsedRdd = rdd.map(line => line.split(","))

  // Define a function to create a sortable key from the specified columns
  def sortableKey(row: Array[String]): String = columns.map(col => row(col)).mkString(",")

  // Sort the RDD based on the sortable key and order
  val sortedRdd = if (ascending) {
    parsedRdd.sortBy(row => sortableKey(row), ascending = true)
  } else {
    parsedRdd.sortBy(row => sortableKey(row), ascending = false)
  }

  // Add rank column and convert back to RDD[String]
  sortedRdd.zipWithIndex.map { case (row, rank) =>
    row.mkString(",") + s",${rank + 1}"
  }
}

}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
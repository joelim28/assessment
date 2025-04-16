error id: java/lang/Object#`==`().
file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol java/lang/Object#
empty definition using fallback
non-local guesses:

offset: 4596
uri: file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
text:
```scala
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
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
      val (rdd_master_stg1, rdd_master_ColNames_stg1) = readParquetFile(spark, parquetFilePath_master)
      // showTopRecords(rdd_master_stg1, rdd_master_ColNames_stg1, 11)  // Ensure master_stg1 is read correctly
    
      /* Clean up Master to ensure no duplicated records based on the Key column geographical_location_oid */
      val (rdd_master_stg2, rdd_master_ColNames_stg2) = dedupeRDD(spark, rdd_master_stg1, rdd_master_ColNames_stg1, Seq("geographical_location_oid"))
      // showTopRecords(rdd_master_stg2, rdd_master_ColNames_stg2, 11)  // Check ensure master_stg2 is transformed correctly
      val (rdd_master_fnl) = filterRecords(rdd_master_stg2, rdd_master_ColNames_stg2, "valid_flag", "Y")
      val rdd_master_ColNames_fnl = rdd_master_ColNames_stg2 // No change to the column names and to ensure consistency of naming convention for maintainability
      //showTopRecords(rdd_master_fnl, rdd_master_ColNames_fnl, 11)  // Check ensure master has no duplicate
       
      //Read and display transaction data
      val (rdd_trnx_stg1, rdd_trnx_ColNames_stg1) = readParquetFile(spark, parquetFilePath_transaction)      
      val (rdd_trnx_stg2, rdd_trnx_ColNames_stg2) = dedupeRDDFirst(spark, rdd_trnx_stg1, rdd_trnx_ColNames_stg1, Seq("detection_oid"), Seq("timestamp_detected"))
      val (rdd_trnx_fnl) = filterRecords(rdd_trnx_stg2, rdd_trnx_ColNames_stg2, "valid_flag", "Y")
      val rdd_trnx_ColNames_fnl = rdd_trnx_ColNames_stg2
      showTopRecords(rdd_trnx_fnl, rdd_trnx_ColNames_fnl, 10)  // Check to ensure data is filtered correctly
      
      // Filter and display records based on column index and filter value
      //val filteredRecords = filterRecords(deduped_rdd_trnx, 2, "28995")
      //showTopRecords(filteredRecords, 10)  // Display top 10 filtered records


      // Perform left join operation
      val (rdd_ods_stg1, rdd_ods_ColNames_stg1) = leftJoinRDDs(spark, rdd_trnx_fnl, rdd_master_fnl, rdd_trnx_ColNames_fnl, rdd_master_ColNames_fnl, "geographical_location_oid", "geographical_location_oid")
      showTopRecords(rdd_ods_stg1, rdd_ods_ColNames_stg1, 10)
      //showTopRecords(rdd_ods_stg1, 10)  // Display top 10 records
 
      val (rdd_ods_fnl, rdd_ods_ColNames_fnl) = keepColumns(rdd_ods_stg1, rdd_ods_ColNames_stg1, Seq("geographical_location_oid", "video_camera_oid","detection_oid","item_name","timestamp_detected","geographical_location"))
      //showTopRecords(rdd_ods, 10)  // Display top 10 records
      showTopRecords(rdd_ods_fnl, rdd_ods_ColNames_fnl, 10)
      
      val (countedRecordsRDD, countedColumnNames) = countRecordsByColumn(rdd_ods_fnl, rdd_ods_ColNames_fnl, Seq("geographical_location", "item_name"))
      showTopRecords(countedRecordsRDD, countedColumnNames, 10)
      
      val (rankedRDD, rankedColumnNames) = addRankColumn(countedRecordsRDD, countedColumnNames, Seq("count"), ascending = false)
      showTopRecords(rankedRDD, rankedColumnNames, 10)
  
  
    } catch {
      case e: java.io.FileNotFoundException => println(s"File not found: ${e.getMessage}")
      case e: org.apache.spark.sql.AnalysisException => println(s"Error reading Parquet file: ${e.getMessage}")
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
 
  // Function to read Parquet file and convert to RDD, returning column names as well
  def readParquetFile(spark: SparkSession, filePath: String): (RDD[String], Seq[String]) = {
    val df = spark.read.parquet(filePath)
    val columnNames = df.columns.toSeq
    val rdd = df.rdd.map(row => row.mkString(","))
    (rdd, columnNames)
  }
  
  import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD

def writeRDDToParquet(spark: SparkSession, rdd: RDD[String], columnNames: Seq[String], outputPath: String): Unit = {
  // Convert RDD to DataFrame
  val df: DataFrame = spark.createDataFrame(
    rdd.map(_.split(",")).map(row@@ => row.toSeq)
  ).toDF(columnNames: _*)

  // Write DataFrame to Parquet file
  df.write.parquet(outputPath)
}



   // Function to filter records based on column name and filter value
  def filterRecords(rdd: RDD[String], columnNames: Seq[String], columnName: String, filterValue: String): RDD[String] = {
    val columnIndex = columnNames.indexOf(columnName)
    rdd.filter(record => record.split(",")(columnIndex) == filterValue)
  }
  
  // Function to display top records from RDD along with header
  def showTopRecords(rdd: RDD[String], columnNames: Seq[String], numRecords: Int): Unit = {
    println(columnNames.mkString(","))
    rdd.take(numRecords).foreach(println)
  }

  // Function to deduplicate RDD based on key column names without sorting
  def dedupeRDD(spark: SparkSession, rdd: RDD[String], columnNames: Seq[String], keyNames: Seq[String]): (RDD[String], Seq[String]) = {
    val keyIndices = keyNames.map(name => columnNames.indexOf(name))
    val dedupedRDD = rdd.map(record => {
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
    val updatedColumnNames = columnNames :+ "valid_flag"
    (dedupedRDD, updatedColumnNames)
  }

  // Function to deduplicate RDD based on key column names, keeping the first occurrence based on sorting sortColumnNames ascending
  def dedupeRDDFirst(spark: SparkSession, rdd: RDD[String], columnNames: Seq[String], keyNames: Seq[String], sortColumnNames: Seq[String]): (RDD[String], Seq[String]) = {
    val keyIndices = keyNames.map(name => columnNames.indexOf(name))
    val sortColumnIndices = sortColumnNames.map(name => columnNames.indexOf(name))

    val dedupedRDD = rdd.map(record => {
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

    val updatedColumnNames = columnNames :+ "valid_flag"
    (dedupedRDD, updatedColumnNames)
  }
 
  // Function to perform a left join of 2 RDDs with rdd1 as left (Transaction & Larger) and rdd2 (Master & lesser) as right. Keyname1 and KeyName2 are the linking key columns correspondingly.
  def leftJoinRDDs(spark: SparkSession, rdd1: RDD[String], rdd2: RDD[String], columnNames1: Seq[String], columnNames2: Seq[String], keyName1: String, keyName2: String): (RDD[String], Seq[String]) = {
    // Get the key indices based on the column names
    val keyIndex1 = columnNames1.indexOf(keyName1)
    val keyIndex2 = columnNames2.indexOf(keyName2)

    // Collect the smaller RDD (master) as a map and broadcast it
    val smallRDDMap = rdd2.map(record => {
      val columns = record.split(",")
      val key = columns.lift(keyIndex2).getOrElse("")
      (key, record)
    }).collectAsMap()

    val broadcastSmallRDD = spark.sparkContext.broadcast(smallRDDMap)

    // Perform the map-side join using the broadcasted RDD
    val resultRDD: RDD[String] = rdd1.map(record => {
      val columns = record.split(",")
      val key = columns.lift(keyIndex1).getOrElse("")
      val joinedRecord = broadcastSmallRDD.value.get(key) match {
        case Some(record2) => s"$record,$record2"
        case None => s"$record,not found"
      }
      joinedRecord
    })

    // Update column names for the joined records
    val updatedColumnNames = columnNames1 ++ columnNames2.map { name =>
      if (columnNames1.contains(name)) s"${name}_joined" else name
    }

    (resultRDD, updatedColumnNames)
  }


  def keepColumns(rdd: RDD[String], columnNames: Seq[String], selectedColumnNames: Seq[String]): (RDD[String], Seq[String]) = {
    // Get the indices of the selected columns based on the column names
    val columnIndices = selectedColumnNames.map(name => columnNames.indexOf(name))

    val resultRDD = rdd.map(record => {
      val columns = record.split(",")
      columnIndices.map(columns(_)).mkString(",")
    })

    (resultRDD, selectedColumnNames)
  }

  def countRecordsByColumn(rdd: RDD[String], columnNames: Seq[String], keyColumnNames: Seq[String]): (RDD[String], Seq[String]) = {
    // Get the indices of the key columns based on the column names
    val keyIndices = keyColumnNames.map(name => columnNames.indexOf(name))

    val resultRDD = rdd.map(record => {
      val columns = record.split(",")
      val key = keyIndices.map(columns(_)).mkString(",")
      (key, 1)
    }).reduceByKey(_ + _)
      .map { case (key, count) => s"$key,$count" }

    val updatedColumnNames = keyColumnNames :+ "count"

    (resultRDD, updatedColumnNames)
  }

  def addRankColumn(rdd: RDD[String], columnNames: Seq[String], keyColumnNames: Seq[String], ascending: Boolean): (RDD[String], Seq[String]) = {
    // Get the indices of the key columns based on the column names
    val keyIndices = keyColumnNames.map(name => columnNames.indexOf(name))

    // Parse the input RDD to extract columns
    val parsedRdd = rdd.map(line => line.split(","))

    // Define a function to create a sortable key from the specified columns
    def sortableKey(row: Array[String]): String = keyIndices.map(row(_)).mkString(",")

    // Sort the RDD based on the sortable key and order
    val sortedRdd = if (ascending) {
      parsedRdd.sortBy(row => sortableKey(row), ascending = true)
    } else {
      parsedRdd.sortBy(row => sortableKey(row), ascending = false)
    }

    // Add rank column and convert back to RDD[String]
    val resultRDD = sortedRdd.zipWithIndex.map { case (row, rank) =>
      row.mkString(",") + s",${rank + 1}"
    }

    val updatedColumnNames = columnNames :+ "rank"

    (resultRDD, updatedColumnNames)
  }


}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 
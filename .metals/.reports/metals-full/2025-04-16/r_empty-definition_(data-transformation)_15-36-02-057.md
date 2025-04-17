error id: org/apache/spark/sql/Row.
file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol org/apache/spark/sql/Row.
empty definition using fallback
non-local guesses:

offset: 5394
uri: file:///C:/Users/josephlim/Projects/data-transformation/src/main/scala/Main.scala
text:
```scala
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD 
import org.apache.log4j.Logger
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import java.nio.file.Files
import org.apache.commons.io.FileUtils

  object ParquetReader {
    def main(args: Array[String]): Unit = {
      if (args.length < 4) {
        println("Usage: ParquetReader <parquetFilePath_master> <parquetFilePath_transaction> <parquetFilePath_output> <TopNRec>")
        System.exit(1)
      }

      val parquetFilePath_master = args(0)
      val parquetFilePath_transaction = args(1)
      val parquetFilePath_output = args(2)
      val TopNRec = args(3).toInt

      val spark = SparkSession.builder
        .appName("Parquet Reader")
        .master("local[*]")
        .config("spark.network.timeout", "300s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
      
      try {
        val (rdd_master_stg1, rdd_master_ColNames_stg1) = readParquetFile(spark, parquetFilePath_master)
        val (rdd_master_stg2, rdd_master_ColNames_stg2) = dedupeRDD(spark, rdd_master_stg1, rdd_master_ColNames_stg1, Seq("geographical_location_oid"))
        val rdd_master_fnl = filterRecords(rdd_master_stg2, rdd_master_ColNames_stg2, "valid_flag", "Y")
        val rdd_master_ColNames_fnl = rdd_master_ColNames_stg2

        val (rdd_trnx_stg1, rdd_trnx_ColNames_stg1) = readParquetFile(spark, parquetFilePath_transaction)
        val (rdd_trnx_stg2, rdd_trnx_ColNames_stg2) = dedupeRDDFirst(spark, rdd_trnx_stg1, rdd_trnx_ColNames_stg1, Seq("detection_oid"), Seq("timestamp_detected"))
        val rdd_trnx_fnl = filterRecords(rdd_trnx_stg2, rdd_trnx_ColNames_stg2, "valid_flag", "Y")
        val rdd_trnx_ColNames_fnl = rdd_trnx_ColNames_stg2
        showTopRecords(rdd_trnx_fnl, rdd_trnx_ColNames_fnl, 10)

        val (rdd_ods_stg1, rdd_ods_ColNames_stg1) = leftJoinRDDs(spark, rdd_trnx_fnl, rdd_master_fnl, rdd_trnx_ColNames_fnl, rdd_master_ColNames_fnl, "geographical_location_oid", "geographical_location_oid")
        showTopRecords(rdd_ods_stg1, rdd_ods_ColNames_stg1, 10)

        val (rdd_ods_fnl, rdd_ods_ColNames_fnl) = keepColumns(rdd_ods_stg1, rdd_ods_ColNames_stg1, Seq("geographical_location_oid", "video_camera_oid", "detection_oid", "item_name", "timestamp_detected", "geographical_location"))

        val (countedRecordsRDD, countedColNames) = countRecordsByColumn(rdd_ods_fnl, rdd_ods_ColNames_fnl, Seq("geographical_location", "item_name"))

        val (rankedRDD, rankedColNames) = addRankColumn(countedRecordsRDD, countedColNames, Seq("count"), ascending = false)
        val (rdd_report, rdd_report_ColNames) = rddTopRecords(rankedRDD, rankedColNames, TopNRec)

        writeParquetFile(spark, rdd_report, rdd_report_ColNames, "parquetFilePath_output", sampleSize = TopNRec)
  //        writeParquetFile(spark, rdd_report, rdd_report_ColNames, parquetFilePath_output)

      } catch {
        case e: java.io.FileNotFoundException => println(s"File not found: ${e.getMessage}")
        case e: org.apache.spark.sql.AnalysisException => println(s"Error reading Parquet file: ${e.getMessage}")
        case e: Exception => println(s"An error occurred: ${e.getMessage}")
      } finally {
        spark.stop()
      }
    }
    // Other functions remain unchanged
   
  // Function to read Parquet file and convert to RDD, returning column names as well
  def readParquetFile(spark: SparkSession, filePath: String): (RDD[String], Seq[String]) = {
    val df = spark.read.parquet(filePath)
    val columnNames = df.columns.toSeq
    val rdd = df.rdd.map(row => row.mkString(","))
    (rdd, columnNames)
  }

	def writeParquetFile(spark: SparkSession, rdd: RDD[String], columnNames: Seq[String], filePath: String, sampleSize: Int = 100): Unit = {
	  // Parse the RDD[String] into RDD[Array[Any]]
	  val parsedRDD = rdd.map { line =>
		val parts = line.split(",") // Assuming CSV format
		parts.zipWithIndex.map {
		  case (value, index) =>
			if (index == 0) value // Assuming the first column is a string
			else if (value.forall(_.isDigit)) value.toInt // Assuming integer if all characters are digits
			else if (value.matches("""\d+\.\d+""")) value.toDouble // Assuming double if it matches a decimal pattern
			else value // Default to string
		}
	  }

	  // Sample rows to infer schema
	  val sampleRows = parsedRDD.take(sampleSize)

	  // Infer schema from sample rows
	  val schema = StructType(columnNames.zipWithIndex.map {
		case (name, index) =>
		  val dataType = sampleRows.map(_(index)).collect {
			case _: String => StringType
			case _: Int => IntegerType
			case _: Long => LongType
			case _: Double => DoubleType
			case _: Boolean => BooleanType
		  }.headOption.getOrElse(StringType) // Default to StringType if unknown
		  StructField(name, dataType, nullable = true)
	  })

	  // Convert RDD to DataFrame
	  val rowRDD = parsedRDD.map(array => Row@@(array: _*))
	  val df = spark.createDataFrame(rowRDD, schema)

	  // Write DataFrame to Parquet
	  df.write.parquet(filePath)
	}



  
   // Function to filter records based on column name and filter value
  def filterRecords(rdd: RDD[String], columnNames: Seq[String], columnName: String, filterValue: String): RDD[String] = {
    val columnIndex = columnNames.indexOf(columnName)
    rdd.filter(record => record.split(",")(columnIndex) == filterValue)
  }

   
  // Function to display top records from RDD along with header and save the result into a new RDD
  def rddTopRecords(rdd: RDD[String], columnNames: Seq[String], numRecords: Int): (RDD[String], Seq[String]) = {
    val header = columnNames.mkString(",")
    val topRecords = rdd.take(numRecords)
    val resultRDD = rdd.sparkContext.parallelize(Seq(header) ++ topRecords)
    (resultRDD, columnNames)
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
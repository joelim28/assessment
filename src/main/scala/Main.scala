/*  Author: Joseph Lim
    Date:  16 April 2025
    Requirements: Reads Data from Parquet Files where the files can be master & transaction.  
                  Perform cleaning be removing duplicated records and perform a count group by 
                  Create Parquet File to store the output.
                  The number of records (TopN), the Input Files and Output Files shall be a parameter passed into the program
    Usage:  sbt "run parquetFilePath_master parquetFilePath_transaction parquetFilePath_output TopNRec"
*/

// Library Section
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD 
import org.apache.log4j.Logger
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils
import scala.util.Try
 
// Main Program
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
       
      // Print all configurations
      spark.conf.getAll.foreach { case (key, value) =>
        println(s"$key = $value")
      }


      try {
      //Extraction of data from source parquet file to rdd.
      //rdd ColNames stores the Column Name from the parquet file
      val (rdd_master_stg1, rdd_master_ColNames_stg1) = readParquetFile(spark, parquetFilePath_master)
      val (rdd_trnx_stg1, rdd_trnx_ColNames_stg1) = readParquetFile(spark, parquetFilePath_transaction)
      
      //Identify records that have duplicate based on Key Column
      //Master will be deduped where the last (latest) record by Key Column in the RDD will be considered as the valid (Validity_Flag = "Y").  
      val (rdd_master_stg2, rdd_master_ColNames_stg2) = dedupeRDD(spark, rdd_master_stg1, rdd_master_ColNames_stg1, Seq("geographical_location_oid"))
      //Transaction will be debuped where the first (earliest) record by Key Column in the RDD will be considered as valid (Validity_Flag = "Y")
      val (rdd_trnx_stg2, rdd_trnx_ColNames_stg2) = dedupeRDDFirst(spark, rdd_trnx_stg1, rdd_trnx_ColNames_stg1, Seq("detection_oid"), Seq("timestamp_detected"))
      
      //Filter only the Valid records (valid_flag = "Y")
      val rdd_master_fnl = filterRecords(rdd_master_stg2, rdd_master_ColNames_stg2, "valid_flag", "Y")
      val rdd_trnx_fnl = filterRecords(rdd_trnx_stg2, rdd_trnx_ColNames_stg2, "valid_flag", "Y")
      val rdd_master_ColNames_fnl = rdd_master_ColNames_stg2 // For consistency of naming convention.
      val rdd_trnx_ColNames_fnl = rdd_trnx_ColNames_stg2
  

      //Combine both Master & Transactions into a single ODS table (keeping all records for transaction in an outer join) so it can be reuse for other compute (grouping by other columns)
      //Retain the column that should be in the ODS to remove other duplicated columns after join
          val (rdd_ods_stg1, rdd_ods_ColNames_stg1) = leftJoinRDDs(spark, rdd_trnx_fnl, rdd_master_fnl, rdd_trnx_ColNames_fnl, rdd_master_ColNames_fnl, "geographical_location_oid", "geographical_location_oid")
          val (rdd_ods_fnl, rdd_ods_ColNames_fnl) = keepColumns(rdd_ods_stg1, rdd_ods_ColNames_stg1, Seq("geographical_location_oid", "video_camera_oid", "detection_oid", "item_name", "timestamp_detected", "geographical_location"))
      
      //Perform the logic calculation based on the ODS - count the number of records group by columns.  An additional column "count" will be added to the return RDD
      //Rank the summarized RDD by descending order.  Have a choice to change to ascending to get the smallest count (least)
          val (countedRecordsRDD, countedColNames) = countRecordsByColumn(rdd_ods_fnl, rdd_ods_ColNames_fnl, Seq("geographical_location", "item_name"))
          val (rankedRDD, rankedColNames) = addRankColumn(countedRecordsRDD, countedColNames, Seq("count"), ascending = false)
      
      //Generate an RDD to store the Top N Record
          val (rdd_report, rdd_report_ColNames) = rddTopRecords(rankedRDD, rankedColNames, TopNRec)
          val (rdd_report_fnl, rdd_report_ColNames_fnl) = keepColumns(rdd_report, rdd_report_ColNames, Seq("geographical_location", "item_name", "rank"))

          //Define the output schema and write the reported data into an output parquet file
      //Define the schema datatype
          val schema = StructType(Seq(
          StructField("geographical_location", StringType, nullable = true),
          StructField("item_name", StringType, nullable = true),
          StructField("rank", IntegerType, nullable = true)
          ))
          writeParquetFile(spark, rdd_report_fnl, schema, parquetFilePath_output)
 

      } catch {
        case e: java.io.FileNotFoundException => println(s"File not found: ${e.getMessage}")
        case e: org.apache.spark.sql.AnalysisException => println(s"Error reading Parquet file: ${e.getMessage}")
        case e: Exception => println(s"An error occurred: ${e.getMessage}")
      } finally {
        spark.stop()
      }
    }

  // The following section are the functions used in the main program.

  // Parquet File Management
  // Function to read Parquet file and convert to RDD, returning column names as well
  // The function will read the source parquet file path & name and return the RDD & Column Names
  def readParquetFile(spark: SparkSession, filePath: String): (RDD[String], Seq[String]) = {
    val df = spark.read.parquet(filePath)
    val columnNames = df.columns.toSeq
    val rdd = df.rdd.map(row => row.mkString(","))
    (rdd, columnNames)
  }

  // Function to write RDD to parquet file. 
  // The function will write the target parquet to the specified directory (filePath)
  def writeParquetFile(spark: SparkSession, rdd: RDD[String], schema: StructType, filePath: String, hasHeader: Boolean = true): Unit = {
    try {
      // Skip the header row if present
      val dataRDD = if (hasHeader) rdd.zipWithIndex.filter(_._2 > 0).map(_._1) else rdd

      // Parse the RDD[String] into RDD[Array[Any]]
      val parsedRDD = dataRDD.map { line =>
        val parts = line.split(",") // Assuming CSV format
        parts.zipWithIndex.map {
          case (value, index) =>
            schema(index).dataType match {
              case StringType => value
              case IntegerType => value.toIntOption.getOrElse(0)
              case LongType => value.toLongOption.getOrElse(0L)
              case DoubleType => value.toDoubleOption.getOrElse(0.0)
              case BooleanType => value.toBooleanOption.getOrElse(false)
              case _ => value // Default to string if unknown
            }
        }
      }

      // Convert RDD to DataFrame
      val rowRDD = parsedRDD.map(array => Row(array: _*))
      val df = spark.createDataFrame(rowRDD, schema)
  
      // Write DataFrame to a single Parquet file under the file path directory
      df.coalesce(1).write.mode("overwrite").parquet(filePath)
      println(s"Data successfully written to $filePath")
    } catch {
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    }
  }

  // Helper methods for safe parsing
  implicit class StringImprovements(val s: String) {
    def toIntOption: Option[Int] = Try(s.toInt).toOption
    def toLongOption: Option[Long] = Try(s.toLong).toOption
    def toDoubleOption: Option[Double] = Try(s.toDouble).toOption
    def toBooleanOption: Option[Boolean] = Try(s.toBoolean).toOption
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
  // This is useful for validation purpose or print the records but do not return RDD
  def showTopRecords(rdd: RDD[String], columnNames: Seq[String], numRecords: Int): Unit = {
    println(columnNames.mkString(","))
    rdd.take(numRecords).foreach(println)
  }

  /* Function to deduplicate RDD based on key column names without sorting
     The function will take in the RDD, ColumnName, Key Column Name as the parameter for identifying duplicates
     A new column (valid_flag) will be returned for each record based on the Key Column as follows:
       "X" for all earlier rows if there are identically duplicated
       "N" for all earlier rows if there are not identically duplicated
       "Y" if there are no duplicates or the lastest record if duplicated
     It will be useful to identify and send "N" to exception to investigate for new duplicated records scenario 
  */
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

  /* Function to deduplicate RDD based on key column names, keeping the first occurrence based on sorting sortColumnNames ascending
     The function will take in the RDD, ColumnName, Key Column Name, sort Column Name as the parameter for identifying duplicates
     A new column (valid_flag) will be returned for each record based on the Key Column and sorted ascending based on the sort Column Names as follows:
       "X" for all later rows if there are identically duplicated 
       "N" for all later rows if there are not identically duplicated
       "Y" if there are no duplicates or the earliest record if duplicated
     It will be useful to identify and send "N" to exception to investigate for new duplicated records scenario 
  */
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
 
  /* Function to perform a left outer join of 2 RDDs with rdd1 as left (Transaction & Larger) and rdd2 (Master & lesser) as right. 
	 Keyname1 and KeyName2 are the linking key columns correspondingly.
  */
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

  // This function will return the RDD and Column Name based on the selected column name passed in as parameter
  def keepColumns(rdd: RDD[String], columnNames: Seq[String], selectedColumnNames: Seq[String]): (RDD[String], Seq[String]) = {
    // Get the indices of the selected columns based on the column names
    val columnIndices = selectedColumnNames.map(name => columnNames.indexOf(name))

    val resultRDD = rdd.map(record => {
      val columns = record.split(",")
      columnIndices.map(columns(_)).mkString(",")
    })

    (resultRDD, selectedColumnNames)
  }

  // This function will count the number of rows in a RDD group by Key Column Name and return a RDD with column Names
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

  // This function will return a RDD, column Names with an additional rank column with the ranking based on the keyColumn to sort the ranking and ascending/descending flag as parameter.
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

  //functions for Distributed Compute II
  //Leverage on Salting to handle data skewed for transaction file.
  //Salting involves adding a random value to the key to distribute the data more evenly across partitions.

  def readParquetFileWithSalting(spark: SparkSession, filePath: String): (RDD[String], Seq[String]) = {
    val df = spark.read.parquet(filePath)
    val columnNames = df.columns.toSeq

    // Add a salt column to the DataFrame
    val saltedDF = df.withColumn("salt", expr("floor(rand() * 10)"))

    // Repartition the DataFrame based on the salted key
    val repartitionedDF = saltedDF.repartition(col("geographical_location"), col("salt"))

    // Convert the repartitioned DataFrame to an RDD of comma-separated strings
    val rdd = repartitionedDF.rdd.map(row => row.mkString(","))

    (rdd, columnNames)
  }

  


}

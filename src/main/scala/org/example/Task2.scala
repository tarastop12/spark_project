package org.example

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object Task2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DocumentOpenAnalyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawData = spark.read
      .option("encoding", "utf-8")
      .option("wholetext", value = true)
      .text("data/sessions/*")
      .withColumn("file_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
      .withColumn("split_file_rows", F.split(F.col("value"), "\n"))
      .selectExpr(
        "*",
        "posexplode(split_file_rows) as (idx, line)"
      )
      .select(
        F.col("file_name").as("filename"),
        F.col("idx"),
        F.col("line")
      )

    val windowSpec = Window.partitionBy("filename").orderBy("idx")

    //QS
    val qsLines = rawData.filter($"line".startsWith("QS "))

    val nextLineDF = rawData.select(
      $"line".as("next_line"),
      $"idx".as("next_idx"),
      $"filename".as("next_filename")
    )

    // джоин QS
    val qsWithDocs = qsLines.join(
        nextLineDF,
        qsLines("idx") + 1 === nextLineDF("next_idx") &&
          qsLines("filename") === nextLineDF("next_filename"),
        "inner"
      )
      .select(
        qsLines("line").as("qs_line"),
        nextLineDF("next_line"),
        nextLineDF("next_filename")
      )
      .withColumn("qs_date", F.split(F.col("qs_line"), " ")(1).substr(0, 10))
      .withColumn("document_ids", F.split($"next_line", " "))
      .select("qs_date", "document_ids")

    // список всех доков
    val qsData = qsWithDocs
      .withColumn("document_id", F.explode($"document_ids"))
      .filter(
        $"document_id" =!= "" &&
          !$"document_id".contains("{") &&
          !$"document_id".contains("}")
      )
      .select("qs_date", "document_id")

    // события DOC_OPEN
    val docOpenData = rawData.filter($"line".startsWith("DOC_OPEN "))
      .select(
        F.split($"line", " ")(1).substr(0, 10).as("open_date"),
        F.split($"line", " ")(3).as("document_id")
      )

    val docOpenDataRenamed = docOpenData.withColumnRenamed("document_id", "doc_open_document_id")

    // джоин QS и DOC_OPEN
    val joined = qsData.join(docOpenDataRenamed,
      qsData("document_id") === docOpenDataRenamed("doc_open_document_id") && //??
        qsData("qs_date") === docOpenDataRenamed("open_date"),
      "inner"
    )

    // гроуп
    val agg = joined.groupBy($"document_id", $"qs_date")
      .agg(F.count("*").as("opens"))

    val result = agg.groupBy("document_id")
      .agg(F.map_from_entries(F.collect_list(F.struct($"qs_date", $"opens"))).as("daily_opens"))
      .collect()

    val output = result.map { row =>
      val documentId = row.getString(0)
      val dailyOpens = row.getMap[String, Long](1)
      Map(
        "document_id" -> documentId,
        "daily_opens" -> dailyOpens
      )
    }

    output.foreach(println)

    spark.stop()
  }
}

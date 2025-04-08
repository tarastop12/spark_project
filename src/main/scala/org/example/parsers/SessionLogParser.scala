package org.example.parsers

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.Window

class SessionLogParser extends LogParser {
  private val windowSpec = Window.partitionBy("filename").orderBy("line_number")

  override def parseRawData(rawData: DataFrame): DataFrame = {
    rawData
      .withColumn("file_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
      .withColumn("split_file_rows", F.split(F.col("value"), "\n"))
      .selectExpr(
        "*",
        "posexplode(split_file_rows) as (row_id, log_message)"
      )
      .select(
        F.col("file_name").as("filename"),
        F.col("row_id").as("line_number"),
        F.col("log_message").as("line")
      )
  }

  override def extractCardSessions(data: DataFrame): DataFrame = {
    data
      .withColumn("is_card_start", F.col("line").startsWith("CARD_SEARCH_START"))
      .withColumn("is_card_end", F.col("line").startsWith("CARD_SEARCH_END"))
      .withColumn("card_flag",
        F.sum(
          F.when(F.col("is_card_start"), 1)
            .when(F.col("is_card_end"), -1)
            .otherwise(0)
        ).over(windowSpec)
      )
      .filter(F.col("card_flag") > 0 || F.col("is_card_start"))
      .filter(!F.col("line").startsWith("CARD_SEARCH_"))
  }

  override def countTargetOccurrences(data: DataFrame, targetId: String): Array[(String, Long)] = {
    import data.sparkSession.implicits._

    data
      .select("line", "filename")
      .as[(String, String)]
      .map { case (line, filename) =>
        (filename, line.split("\\s+").count(_ == targetId).toLong)
      }
      .rdd
      .reduceByKey(_ + _)
      .filter(_._2 > 0)
      .collect()
  }
}
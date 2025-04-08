package org.example.services

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.models.AnalysisResult
import org.example.parsers.LogParser
import scala.util.{Try, Success, Failure}

class AnalysisService(parser: LogParser, spark: SparkSession) {
  import spark.implicits._

  def analyze(targetId: String, inputPath: String): Either[String, AnalysisResult] = {
    Try {
      val rawData = spark.read
        .option("encoding", "utf-8")
        .option("wholetext", value = true)
        .text(inputPath)

      val parsedData = parser.parseRawData(rawData)
      val cardSessions = parser.extractCardSessions(parsedData)
      val counts = parser.countTargetOccurrences(cardSessions, targetId)

      AnalysisResult(
        documentId = targetId,
        totalCount = counts.map(_._2).sum,
        fileCounts = counts.toMap
      )
    }.toEither.left.map(_.getMessage)
  }
}
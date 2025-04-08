package org.example

import org.example.models.AnalysisResult
import org.example.parsers.SessionLogParser
import org.example.services.AnalysisService
import org.example.utils.SparkSessionBuilder


object Main extends App {
  private val targetId = "ACC_45616"
  private val inputPath = "data/sessions"

  private val spark = SparkSessionBuilder.build("DocumentAnalyzer")
  private val parser = new SessionLogParser()
  private val analysisService = new AnalysisService(parser, spark)

  analysisService.analyze(targetId, inputPath) match {
    case Right(result) => printResults(result)
    case Left(error) => println(s"Analysis failed: $error")
  }

  spark.stop()

  private def printResults(result: AnalysisResult): Unit = {
    println(s"Document ID: ${result.documentId}")
    println(s"Total count: ${result.totalCount}")
    println("Counts by file:")
    result.fileCounts.foreach { case (file, count) =>
      println(s"  $file: $count")
    }
  }
}
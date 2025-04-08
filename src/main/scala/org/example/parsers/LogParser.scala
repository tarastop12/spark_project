package org.example.parsers

import org.apache.spark.sql.DataFrame

trait LogParser {

  def parseRawData(rawData: DataFrame): DataFrame

  def extractCardSessions(data: DataFrame): DataFrame

  def countTargetOccurrences(data: DataFrame, targetId: String): Array[(String, Long)]
}
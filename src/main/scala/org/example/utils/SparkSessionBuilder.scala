package org.example.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build(appName: String): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
  }
}
package org.example.models

case class AnalysisResult(
                           documentId: String,
                           totalCount: Long,
                           fileCounts: Map[String, Long]
                         )
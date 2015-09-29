package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.actor.ActorRef
import akka.persistence.journal.JournalPerfSpec
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.config.ConfigFactory
import org.elasticsearch.common.settings.ImmutableSettings
import scala.concurrent.duration._

class ElasticsearchAsyncWriteJournalPerfSpect extends JournalPerfSpec(
    config = ConfigFactory.parseString(
      s"""
      akka {
        test.single-expect-default = 10s
        persistence.journal.plugin = "elasticsearch-journal"
        loggers = ["akka.event.slf4j.Slf4jLogger"]
        debug.receive = on
        loglevel = "DEBUG"
      }

      elasticsearch-persistence {
          local = true
          cluster = "journal_test"
          index = "akkajournal"
       }

       elasticsearch-journal {
          class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticsearchAsyncWriteJournal"
       }
    """
    )) with ElasticsearchSetup {

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
    esClient.admin.indices().prepareRefresh("akkajournal").execute().actionGet()
  }

}

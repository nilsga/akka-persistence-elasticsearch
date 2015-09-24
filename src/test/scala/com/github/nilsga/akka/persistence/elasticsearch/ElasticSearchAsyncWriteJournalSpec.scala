package com.github.nilsga.akka.persistence.elasticsearch

import akka.actor.ActorRef
import akka.persistence.journal.JournalSpec
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.config._

class ElasticSearchAsyncWriteJournalSpec extends JournalSpec(
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
          class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticSearchAsyncWriteJournal"
       }
    """
  )) with ElasticSearchSetup {

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
    esClient.execute(refresh index "akkajournal").await
  }

}

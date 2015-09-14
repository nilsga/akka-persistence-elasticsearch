package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.actor.ActorRef
import akka.persistence.journal.JournalSpec
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.config._
import org.elasticsearch.common.settings.ImmutableSettings

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

      elasticsearch-journal {
          class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticSearchAsyncWriteJournal"
          local = true
          cluster = "journal_test"
          index = "akkajournal"
       }
    """
  )) {
  val dataDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(UUID.randomUUID().toString)
  println("Using " + dataDir + " as home")
  dataDir.toFile.deleteOnExit()
  dataDir.toFile.mkdirs()
  val esClient = ElasticClient.local(ImmutableSettings.settingsBuilder()
    .put("path.home", dataDir.toFile.getAbsolutePath)
    .put("path.repo", dataDir.toFile.getAbsolutePath)
    .put("index.number_of_shards", 1)
    .put("index.number_of_replicas", 0).build())


  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
    esClient.admin.indices().prepareRefresh("akkajournal").execute().actionGet()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    esClient.close()
  }
}

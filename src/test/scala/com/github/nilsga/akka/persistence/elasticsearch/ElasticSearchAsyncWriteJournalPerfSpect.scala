package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.actor.ActorRef
import akka.persistence.journal.JournalPerfSpec
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.config.ConfigFactory
import org.elasticsearch.common.settings.ImmutableSettings
import scala.concurrent.duration._

class ElasticSearchAsyncWriteJournalPerfSpect extends JournalPerfSpec(
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
    )) {

    val dataDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(UUID.randomUUID().toString)
    dataDir.toFile.deleteOnExit()
    dataDir.toFile.mkdirs()
    val esClient = ElasticClient.local(ImmutableSettings.settingsBuilder()
      .put("path.home", dataDir.toFile.getAbsolutePath)
      .put("path.repo", dataDir.toFile.getAbsolutePath)
      .put("threadpool.bulk.queue_size", 10000)
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 0).build())


  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
    esClient.admin.indices().prepareRefresh("akkajournal").execute().actionGet()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    esClient.close()
  }

}
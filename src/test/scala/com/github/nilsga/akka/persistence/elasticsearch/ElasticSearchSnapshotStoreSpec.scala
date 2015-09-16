package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.persistence.SnapshotMetadata
import akka.persistence.snapshot.SnapshotStoreSpec
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.config.ConfigFactory
import org.elasticsearch.common.settings.ImmutableSettings
import com.sksamuel.elastic4s.ElasticDsl._

import scala.collection.immutable.Seq

class ElasticSearchSnapshotStoreSpec extends SnapshotStoreSpec(
  config = ConfigFactory.parseString(
    s"""
      akka {
        test.single-expect-default = 10s
        persistence.snapshot-store.plugin = "elasticsearch-snapshot-store"
        loggers = ["akka.event.slf4j.Slf4jLogger"]
        debug.receive = on
        loglevel = "DEBUG"
      }

      elasticsearch-persistence {
          local = true
          cluster = "journal_test"
          index = "akkajournal"
       }

       elasticsearch-snapshot-store {
          class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticSearchSnapshotStore"
       }
    """
  )) {

  val dataDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(UUID.randomUUID().toString)
  dataDir.toFile.deleteOnExit()
  dataDir.toFile.mkdirs()
  val esClient = ElasticClient.local(ImmutableSettings.settingsBuilder()
    .put("path.home", dataDir.toFile.getAbsolutePath)
    .put("path.repo", dataDir.toFile.getAbsolutePath)
    .put("index.number_of_shards", 1)
    .put("index.number_of_replicas", 0).build())

  override def writeSnapshots(): Seq[SnapshotMetadata] = {
    val snaps = super.writeSnapshots()
    esClient.execute(refresh index "akkajournal").await
    snaps
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    esClient.close()
  }
}

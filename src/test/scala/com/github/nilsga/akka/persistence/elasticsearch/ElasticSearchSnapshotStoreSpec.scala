package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.SnapshotMetadata
import akka.persistence.snapshot.SnapshotStoreSpec
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Seq

class ElasticsearchSnapshotStoreSpec extends SnapshotStoreSpec(
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
          class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticsearchSnapshotStore"
       }
    """
  )) with ElasticsearchSetup {

  override def writeSnapshots(): Seq[SnapshotMetadata] = {
    val snaps = super.writeSnapshots()
    esClient.execute(refresh index "akkajournal").await
    snaps
  }
}

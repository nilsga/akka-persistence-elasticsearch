package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.persistence.PluginSpec
import com.sksamuel.elastic4s.ElasticClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.scalatest.{Suite, BeforeAndAfterAll}


trait ElasticsearchSetup extends BeforeAndAfterAll { this: PluginSpec =>

  var esClient : ElasticClient = null

  override protected def beforeAll(): Unit = {
    val dataDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(UUID.randomUUID().toString)
    dataDir.toFile.deleteOnExit()
    dataDir.toFile.mkdirs()
    esClient = ElasticClient.local(ImmutableSettings.settingsBuilder()
      .put("path.home", dataDir.toFile.getAbsolutePath)
      .put("path.repo", dataDir.toFile.getAbsolutePath)
      .put("threadpool.bulk.queue_size", 10000)
      .put("cluster.routing.allocation.disk.threshold_enabled", false)
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 0).build())
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    esClient.close()
    super.afterAll()
  }
}

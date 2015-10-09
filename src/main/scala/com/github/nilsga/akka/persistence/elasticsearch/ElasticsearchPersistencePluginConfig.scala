package com.github.nilsga.akka.persistence.elasticsearch

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import com.typesafe.config.Config
import org.elasticsearch.common.settings.ImmutableSettings
import scala.collection.JavaConversions._


object ElasticsearchPersistencePluginConfig {

  def apply(system: ActorSystem) = new ElasticsearchPersistencePluginConfig(system.settings.config.getConfig("elasticsearch-persistence"))

}

class ElasticsearchPersistencePluginConfig(config: Config) {
  val journalType = "journal"
  val snapshotType = "snapshot"
  val index = config.getString("index")
  val cluster = config.getString("cluster")
  def createClient = config.hasPath("local") && config.getBoolean("local") match {
    case true =>
      ElasticClient.local(ImmutableSettings.settingsBuilder().put("node.data", false).put("node.master", false).build())
    case false =>
      val esSettings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build()
      val nodes = config.getStringList("nodes").map(node => if(node.indexOf(":") >= 0) node else s"$node:9300")
      val connectionString = s"elasticsearch://${nodes.mkString(",")}"
      ElasticClient.remote(esSettings, ElasticsearchClientUri(connectionString))
  }
}

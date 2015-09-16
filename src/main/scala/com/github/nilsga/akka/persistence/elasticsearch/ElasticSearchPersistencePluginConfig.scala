package com.github.nilsga.akka.persistence.elasticsearch

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import com.typesafe.config.Config
import org.elasticsearch.common.settings.ImmutableSettings


object ElasticSearchPersistencePluginConfig {

  def apply(system: ActorSystem) = new ElasticSearchPersistencePluginConfig(system.settings.config.getConfig("elasticsearch-persistence"))

}

class ElasticSearchPersistencePluginConfig(config: Config) {
  val journalType = "journal"
  val snapshotType = "snapshot"
  val index = config.getString("index")
  val cluster = config.getString("cluster")
  def createClient = config.hasPath("local") && config.getBoolean("local") match {
    case true =>
      println("Create local client")
      ElasticClient.local(ImmutableSettings.settingsBuilder().put("node.data", false).put("node.master", false).build())
    case false =>
      println("Create remote client")
      val esSettings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build()
      val uri = config.getString("url")
      ElasticClient.remote(esSettings, ElasticsearchClientUri(uri))
  }
}

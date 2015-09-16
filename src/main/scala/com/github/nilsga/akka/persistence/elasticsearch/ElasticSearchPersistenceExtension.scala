package com.github.nilsga.akka.persistence.elasticsearch

import akka.actor._
import com.sksamuel.elastic4s.ElasticClient

object ElasticSearchPersistenceExtension extends ExtensionId[ElasticSearchPersistenceExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): ElasticSearchPersistenceExtensionImpl = new ElasticSearchPersistenceExtensionImpl(system)

  override def lookup() = ElasticSearchPersistenceExtension
}

class ElasticSearchPersistenceExtensionImpl(val system: ActorSystem) extends Extension {
  val config = ElasticSearchPersistencePluginConfig(system)
  lazy val client : ElasticClient = config.createClient
}

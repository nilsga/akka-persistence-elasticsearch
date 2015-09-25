package com.github.nilsga.akka.persistence.elasticsearch

import java.util.concurrent.Executors

import akka.actor._
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import com.sksamuel.elastic4s.ElasticClient

object ElasticSearchPersistenceExtension extends ExtensionId[ElasticSearchPersistenceExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): ElasticSearchPersistenceExtensionImpl = new ElasticSearchPersistenceExtensionImpl(system)

  override def lookup() = ElasticSearchPersistenceExtension
}

class ElasticSearchPersistenceExtensionImpl(val system: ActorSystem) extends Extension {

  private val createTimeout = 10 seconds
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private implicit val plugin = this
  val config = ElasticSearchPersistencePluginConfig(system)
  lazy val client : ElasticClient = config.createClient

  Await.result(ElasticSearchPersistenceMappings.ensureJournalMappingExists(), createTimeout)
  Await.result(ElasticSearchPersistenceMappings.ensureSnapshotMappingExists(), createTimeout)

}

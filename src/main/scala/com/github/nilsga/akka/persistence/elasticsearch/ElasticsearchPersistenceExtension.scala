package com.github.nilsga.akka.persistence.elasticsearch

import java.util.concurrent.Executors

import akka.actor._
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import com.sksamuel.elastic4s.ElasticClient

object ElasticsearchPersistenceExtension extends ExtensionId[ElasticsearchPersistenceExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): ElasticsearchPersistenceExtensionImpl = new ElasticsearchPersistenceExtensionImpl(system)

  override def lookup() = ElasticsearchPersistenceExtension
}

class ElasticsearchPersistenceExtensionImpl(val system: ActorSystem) extends Extension {

  private val createTimeout = 10 seconds
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private implicit val plugin = this
  val config = ElasticsearchPersistencePluginConfig(system)
  lazy val client : ElasticClient = config.createClient

  Await.result(ElasticsearchPersistenceMappings.ensureJournalMappingExists(), createTimeout)
  Await.result(ElasticsearchPersistenceMappings.ensureSnapshotMappingExists(), createTimeout)

}

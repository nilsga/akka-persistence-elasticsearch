package com.github.nilsga.akka.persistence.elasticsearch

import akka.actor.DiagnosticActorLogging
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.SerializationExtension
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.common.Base64
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.aggregations.metrics.max.Max

import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


class ElasticSearchAsyncWriteJournal extends AsyncWriteJournal with ElasticSearchAsyncRecovery with DiagnosticActorLogging {

  import context._

  val pluginConfig = context.system.settings.config.getConfig("elasticsearch-journal")

  val esCluster = pluginConfig.getString("cluster")
  val esSettings = ImmutableSettings.settingsBuilder().put("cluster.name", esCluster).build()
  val esClient = pluginConfig.hasPath("local") match {
    case true =>
      ElasticClient.local(ImmutableSettings.settingsBuilder().put("node.data", false).put("node.master", false).build())
    case false =>
      val uri = pluginConfig.getString("url")
      ElasticClient.remote(esSettings, ElasticsearchClientUri(uri))
  }
  val journalIndex = pluginConfig.getString("index")
  val serializer = SerializationExtension(context.system)
  val journalType = "representation"


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    val journalIndexExists = esClient.admin.indices().prepareExists(journalIndex).execute().actionGet().isExists
    if(!journalIndexExists) {
      val mapping = JournalMapping().toString
      esClient.admin.indices().prepareCreate(journalIndex).addMapping(journalType, JournalMapping().mapping).execute().actionGet()
    }
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    log.debug("Writing {} messages to journal", messages.length)
    val responses: Seq[Future[BulkResponse]] = messages.map(write => {
      try {
        val indexRequests = write.payload.map(pr => {
          index into journalIndex / journalType id s"${pr.persistenceId}-${pr.sequenceNr}" fields(
            "persistenceId" -> pr.persistenceId,
            "sequenceNumber" -> pr.sequenceNr,
            "message" -> serializer.serialize(pr).get
            )
        })
        esClient execute bulk(indexRequests)
      }
      catch {
        case NonFatal(ex) => Future.failed(ex)
      }
    })

    Future.sequence(responses.map(bf => bf.map(br => br.hasFailures match {
      case false => Success()
      case true => Failure(new RuntimeException(br.buildFailureMessage()))
    }).recover({
      case NonFatal(ex) => Failure(ex)
    })))
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("Deleting messages for {} to seqnr {} ", persistenceId, toSequenceNr)
    val messagesToDelete = esClient.publisher(search in journalIndex / journalType sourceInclude ("persistenceId", "sequenceNumber") query {
      filteredQuery filter {
        and(
          termFilter("persistenceId", persistenceId),
          rangeFilter("sequenceNumber").lte(toSequenceNr.toString)
        )
      }
    } scroll "1m")

    val promise = Promise[Unit]

    val reqBuilder = new RequestBuilder[RichSearchHit] {
      override def request(t: RichSearchHit): BulkCompatibleDefinition = {
        log.debug("Deleting message {} {}", t.id, t.sourceAsString)
        delete id t.id from journalIndex / journalType
      }
    }

    val subscriber = esClient.subscriber[RichSearchHit](100, 1, completionFn = () => {
      promise.success()
    }
      , errorFn = (ex) => promise.failure(ex))(reqBuilder, context.system)

    messagesToDelete.subscribe(subscriber)

    promise.future
  }
}

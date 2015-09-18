package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.AtomicWrite
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import org.elasticsearch.action.bulk.BulkResponse

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


class ElasticSearchAsyncWriteJournal extends AsyncWriteJournal with ElasticSearchAsyncRecovery {

  import context._

  implicit val extension = ElasticSearchPersistenceExtension(context.system)

  val journalIndex = extension.config.index
  val journalType = extension.config.journalType
  val serializer = SerializationExtension(context.system)
  val esClient = extension.client

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    Await.result(ElasticSearchPersistenceMappings.ensureJournalMappingExists(), 5 seconds)
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
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

    Future.sequence(responses.map(_.map(bulkResponse => bulkResponse.hasFailures match {
      case false => Success()
      case true => Failure(new RuntimeException(bulkResponse.buildFailureMessage()))
    }).recover({
      case NonFatal(ex) => Failure(ex)
    })))
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val messagesToDelete = esClient.publisher(search in journalIndex / journalType sourceInclude ("_id") query {
      filteredQuery filter {
        and(
          termFilter("persistenceId", persistenceId),
          rangeFilter("sequenceNumber").lte(toSequenceNr.toString)
        )
      }
    } scroll "10s")

    val promise = Promise[Unit]

    val reqBuilder = new RequestBuilder[RichSearchHit] {
      override def request(t: RichSearchHit): BulkCompatibleDefinition = {
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

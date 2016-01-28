package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.AtomicWrite
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexResponse

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


class ElasticsearchAsyncWriteJournal extends AsyncWriteJournal with ElasticsearchAsyncRecovery {

  import context._

  implicit val extension = ElasticsearchPersistenceExtension(context.system)

  val journalIndex = extension.config.index
  val journalType = extension.config.journalType
  val serializer = SerializationExtension(context.system)
  val esClient = extension.client

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    log.debug("Writing {}", messages)
    val responses: Seq[Future[AnyRef]] = messages.map(write => {
      try {
        write.payload.size match {
          case count if count > 1 =>
            log.debug("Bulk write not supported for {}", write)
            Future.failed(new UnsupportedOperationException)
          case _ =>

            val indexRequest = write.payload.map(pr => {
              index into journalIndex / journalType id s"${pr.persistenceId}-${pr.sequenceNr}" fields(
                "persistenceId" -> pr.persistenceId,
                "sequenceNumber" -> pr.sequenceNr,
                "message" -> serializer.serialize(pr).get,
                "deleted" -> false
                )
            }).head
            esClient execute indexRequest
        }
      }
      catch {
        case NonFatal(ex) => Future.failed(ex)
      }
    })

    Future.sequence(responses.map(_.map({
      case indexResponse : IndexResponse =>
        indexResponse.isCreated match {
          case true => Success()
          case false => Failure(new RuntimeException("Error persisting write"))
        }
      case ex : UnsupportedOperationException =>
        Failure(ex)

    }).recover({
      case NonFatal(ex) => Failure(ex)
    })))
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val messagesToDelete = esClient.publisher(search in journalIndex / journalType sourceInclude ("_id") query {
      filteredQuery filter {
        and(
          termFilter("persistenceId", persistenceId),
          rangeFilter("sequenceNumber") lte toSequenceNr.toString
        )
      }
    } scroll "10s")

    val promise = Promise[Unit]

    val reqBuilder = new RequestBuilder[RichSearchHit] {
      override def request(t: RichSearchHit): BulkCompatibleDefinition = {
        update id t.id in journalIndex / journalType doc "deleted" -> true
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

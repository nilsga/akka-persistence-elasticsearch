package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, RichSearchHit}
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.common.Base64
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConversions
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class ElasticSearchSnapshotStore extends SnapshotStore {

  import context._
  implicit val extension = ElasticSearchPersistenceExtension(system)
  val serializer = SerializationExtension(system)
  val esClient = extension.client
  val persistenceIndex = extension.config.index
  val snapshotType = extension.config.snapshotType

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    Await.result(ElasticSearchPersistenceMappings.ensureSnapshotMappingExists(), 5 seconds)
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val query = search in persistenceIndex / snapshotType query {
      filteredQuery filter {
        and(
          termFilter("persistenceId", persistenceId),
          rangeFilter("sequenceNumber") gte criteria.minSequenceNr.toString lte criteria.maxSequenceNr.toString,
          rangeFilter("timestamp") gte criteria.minTimestamp.toString lte criteria.maxTimestamp.toString
        )
      }
    } sourceInclude("_id") sort(field sort "timestamp" order SortOrder.DESC)

    esClient.execute(query).flatMap(searchResponse => {
      val gets = searchResponse.hits.map(get id _.id from persistenceIndex / snapshotType)
      esClient.execute(multiget(gets))
    }).map(multiGetResponse => {
      val responses = JavaConversions.asScalaIterator(multiGetResponse.iterator()).map(_.getResponse)
      responses.collectFirst {
        case r if r.isExists => toSelectedSnapshot(r)
      }
    })
  }

  private def toSelectedSnapshot(response : GetResponse) = {
    val source = response.getSourceAsMap
    val persistenceId = source.get("persistenceId").asInstanceOf[String]
    val sequenceNr = source.get("sequenceNumber").asInstanceOf[Number].longValue()
    val timestamp = source.get("timestamp").asInstanceOf[Number].longValue()
    val snapshotB64 = source.get("snapshot").asInstanceOf[String]
    val snapshot = serializer.deserialize[Snapshot](Base64.decode(snapshotB64), classOf[Snapshot]).get.data
    SelectedSnapshot(SnapshotMetadata(persistenceId, sequenceNr, timestamp), snapshot)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val snapshotId = s"${metadata.persistenceId}-${metadata.sequenceNr}"

    esClient.execute(update id snapshotId in persistenceIndex / snapshotType docAsUpsert(
      "persistenceId" -> metadata.persistenceId,
      "sequenceNumber" -> metadata.sequenceNr,
      "timestamp" -> metadata.timestamp,
      "snapshot" -> serializer.serialize(Snapshot(snapshot)).get
      )
    ).map(_ => Unit)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val snapshotId = s"${metadata.persistenceId}-${metadata.sequenceNr}"
    esClient.execute(delete id snapshotId from persistenceIndex / snapshotType).map(_ => Unit)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val snapshotsToDelete = esClient.publisher(search in persistenceIndex / snapshotType sourceInclude ("_id") query {
      filteredQuery filter {
        and(
          termFilter("persistenceId", persistenceId),
          rangeFilter("sequenceNumber").gte(criteria.minSequenceNr.toString).lte(criteria.maxSequenceNr.toString),
          rangeFilter("timestamp").gte(criteria.minTimestamp.toString).lte(criteria.maxTimestamp.toString)
        )
      }
    } scroll "1m")

    val reqBuilder = new RequestBuilder[RichSearchHit] {
      override def request(t: RichSearchHit): BulkCompatibleDefinition = {
        delete id t.id from persistenceIndex / snapshotType
      }
    }

    val promise = Promise[Unit]

    val subscriber = esClient.subscriber[RichSearchHit](100, 1, completionFn = () => {
      promise.success()
    }
      , errorFn = (ex) => promise.failure(ex))(reqBuilder, context.system)

    snapshotsToDelete.subscribe(subscriber)

    promise.future
  }
}

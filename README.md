# Elasticsearch distributed journal for Akka persistence

 ![Travis build status](https://travis-ci.org/nilsga/akka-persistence-elasticsearch.svg?branch=master)

Akka persistence distributed journal based on Elasticsearch.

## Configuration

```
akka.persistence.journal.plugin = "elasticsearch-journal"
akka.persistence.snapshot-store.plugin = "elasticsearch-snapshot-store"

elasticsearch-journal {
  class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticSearchAsyncWriteJournal"
}

elasticsearch-snapshot-store {
  class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticSearchSnapshotStore"
}

elasticsearch-persistence {
  nodes = ["localhost"]
  cluster = "mycluster"
  index = "akkajournal"
}
```

Note: You don't need both the journal and the snapshot store plugin. Just add the plugin that your application is using.

* `elasticsearch-persistence.nodes` is an array of addresses to the ES master nodes of the cluster
* `elasticsearch-persistence.cluster` is the name of the ES cluster to join
* `elasticsearch-persistence.index` is the name of the index to use for the journal

## Why would I use a search engine as a journal?

You probably wouldn't, unless you already have ES as a part of your infrastructure, and don't want to introduce yet another component.

## Versions

This version of `akka-persistence-elasticsearch` requires akka 2.4 and elasticsearch 1.7. It _might_ work with other elasticsearch versions, but it has not been tested... yet...

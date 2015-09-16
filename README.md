# Elasticsearch distributed journal for Akka persistence

 ![Travis build status](https://travis-ci.org/nilsga/akka-persistence-elasticsearch.svg)

Akka persistence distributed journal based on Elasticsearch.

## Configuration

```
akka.persistence.journal.plugin = "elasticsearch-journal"

elasticsearch-journal {
  class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticSearchAsyncWriteJournal"
  url = "elasticsearch://localhost:9300"
  cluster = "mycluster"
  index = "akkajournal"
}
```

* `elasticsearch-journal.url` is a remote Elasticsearch url as specified for the library [elastic4s](https://github.com/sksamuel/elastic4s#client)
* `elasticsearch-journal.cluster` is the name of the ES cluster to join
* `elasticsearch-journal.index` is the name of the index to use for the journal

## Why would I use a search engine as a journal?

You probably wouldn't, unless you already have ES as a part of your infrastructure, and don't want to introduce yet another component.

## Versions

This version of `akka-persistence-elasticsearch` requires akka 2.4 and elasticsearch 1.7. It _might_ work with other elasticsearch versions, but it has not been tested... yet...

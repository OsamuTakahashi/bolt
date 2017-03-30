# Bolt

The wrapper to use INSERT/UPDATE/DELETE sql query on Google Cloud Spanner.

## How to

In Scala,

Describe library dependency to .sbt file

```
libraryDependencies += "com.sopranoworks" %% "bolt" % "0.1"
```
NOTICE: Bolt currently is not registered to any maven repository.

And then,

```scala
import Bolt._

dbClinet.executeSql("INSERT INTO test_tbl01 VALUES(103,'test insert');")
```

It uses implicit conversion.

## Limitations

* Not support UNION/JOIN
* Not support functions
* Not support complex query

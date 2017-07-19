# Bolt

The wrapper for using INSERT/UPDATE/DELETE sql query on Google Cloud Spanner.

## How to

### In Scala,

Describe library dependency to .sbt file

```
libraryDependencies += "com.sopranoworks" %% "bolt" % "0.9-SNAPSHOT"
```
NOTICE: Bolt currently is not registered to any maven repository.

And then,

```scala
import Bolt._

dbClinet.executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")
```

It uses implicit conversion.

You can use it as explicitly like this.

```scala
Nat(dbClient).executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")

```


#### Other functions

ResultSet is able to be used as Iterator

```scala
dbClient.executeQuery("SELECT * FROM test_tbl01").map {
  resultSet =>
    (resultSet.getString("id"),resultSet.getString("name"))
}
```

And also

```scala
dbClient.executeQuery("SELECT * FROM test_tbl01").headOption
```


### In Java,

...


## Limitations

* Alias is not currently supported in INSERT/UPDATE query
* Array expression is currently not supported in INSERT/UPDATE query
* Only few functions are usable in INSERT/UPDATE query

There is no such a limitation in SELECT and subquery.

## Licence

MIT
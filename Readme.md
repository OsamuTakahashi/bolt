# Bolt

The wrapper for using INSERT/UPDATE/DELETE sql query on Google Cloud Spanner.

## How to

### In Scala,

Describe library dependency to .sbt file

```
libraryDependencies += "com.sopranoworks" %% "bolt" % "0.8"
```
NOTICE: Bolt currently is not registered to any maven repository.

And then,

```scala
import Bolt._

dbClinet.executeSql("INSERT INTO test_tbl01 VALUES(103,'test insert');")
```

It uses implicit conversion.

#### Other functions

ResultSet is able to be used as Iterator

```scala
dbClient.executeSql("SELECT * FROM test_tbl01").map {
  resultSet =>
    (resultSet.getString("id"),resultSet.getString("name"))
}
```

And also

```scala
dbClient.executeSql("SELECT * FROM test_tbl01").headOption
```


### In Java,

...


## Limitations

* Multi primary key is currently not supported on UPDATE
* Only simple 'where clause' is able to be used on INSERT/UPDATE
* Complex query is not supported on INSERT/UPDATE 
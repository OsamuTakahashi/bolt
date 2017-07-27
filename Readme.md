# Bolt

The wrapper for executing INSERT/UPDATE/DELETE sql query on Google Cloud Spanner.

### INSERT/UPDATE/DELETE Syntax

#### INSERT
![INSERT](images/insert.png)

#### UPDATE
![INSERT](images/update.png)

#### DELETE
![INSERT](images/delete.png)

## How to use

### In Scala,

Describe library dependency to .sbt file

```
resolvers += "RustyRaven" at "http://rustyraven.github.io"

libraryDependencies += "com.sopranoworks" %% "bolt" % "0.13"
```

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

Transaction query is able to be used as

```scala
dbClient.beginTransaction {
  tr =>
    tr.executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")
}
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

You can use ResultSet safely like this.

```scala
dbClient.executeQuery("SELECT * FROM test_tbl01").autoclose(
  resultSet =>
      ...
)
```


### In Java,

```java
import com.sopranoworks.bolt.Bolt;

Bolt.Nat nat = Bolt.Nat(client);
nat.executeQuery("INSERT INTO TEST_TABLE (ID,NAME) VALUES(101,'test');");
```

### spanner-cli

This project also contains console spanner client application, spanne-cli.

### spanner-dump

This is a bonus program.
It will dump a spanner database to sql text code.


## Notice

A Nat instance is not thread safe.
You must create a instance per thread.


## Limitations

* Bytes type is currently not supported in INSERT/UPDATE query 
* Only few functions are usable in INSERT/UPDATE query
* CASE Conditional expression is currently not supported in INSERT/UPDATE query

There is no such a limitation in SELECT and subquery.

* Some lterals are currently not supported 

## Licence

MIT
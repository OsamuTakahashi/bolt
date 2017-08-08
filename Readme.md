# Bolt

The wrapper library for executing INSERT/UPDATE/DELETE sql query on Google Cloud Spanner.

### INSERT/UPDATE/DELETE Syntax

#### INSERT
![INSERT](images/insert.png)

#### UPDATE
![UPDATE](images/update.png)

#### DELETE
![DELETE](images/delete.png)

## How to use

### In Scala,

Write library dependency to .sbt file

```
libraryDependencies += "com.sopranoworks" %% "bolt" % "0.14"
```

And then,

```scala
import Bolt._

dbClinet.executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")
```

It uses implicit conversion.

You can use it as explicitly like this.

```scala
Nut(dbClient).executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")

```

A transaction query can be used as this.

```scala
dbClient.beginTransaction {
  tr =>
    tr.executeQuery("INSERT INTO test_tbl01 VALUES(103,'test insert');")
}
```


#### Other functions

ResultSet can be used as Iterator.

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

#### More examples

See NutTest.scala


### In Java,

```java
import com.sopranoworks.bolt.Bolt;

Bolt.Nut nut = Bolt.Nut(client);
nut.executeQuery("INSERT INTO TEST_TABLE (ID,NAME) VALUES(101,'test');");
```

### spanner-cli

This project also contains console spanner client application, spanne-cli.

dist/spanner-cli-0.15-SNAPSHOT.zip

MD5:c1005e19f6456d6af1c659295b86ac0b

### spanner-dump

This is a bonus program.
It will dump a spanner database to sql text code.

dist/spanner-dump-0.15-SNAPSHOT.zip

MD5:7a6f628d6f4ddf34ac51f5c6f9534662

## Notice

A Nut instance is not thread safe.
You must create a instance per thread.


## Limitations

* Bytes type is currently not supported in an INSERT/UPDATE query. 
* Only few functions are usable in an INSERT/UPDATE query.
* CASE Conditional expression is currently not supported in an INSERT/UPDATE query.
* UNNEST operation is currently not supported in an INSERT/UPDATE query.

There is no such a limitation in SELECT and subquery.

* Referencing outside (INSERT/UPDATE statement's) alias in subquery is not supported in an INSERT/UPDATE query.
* Some lterals are currently not supported.

## Licence

MIT
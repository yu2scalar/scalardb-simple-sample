# ScalarDB Cluster - Simple Sample

This is a set of simple examples to demonstrate how to use the ScalarDB Cluster Java API.

Each example is a self-contained class with its own `main` method and its own Gradle task,
so you can read any one of them on its own.

## Prerequisites

- ScalarDB Cluster environment version 3.19.0 or later
- JDK 17
- Gradle 8.8 or later (the Gradle wrapper in this repository is 8.8)

## Configuration

`scalardb.properties` resolves its values from environment variables using ScalarDB's
`${env:NAME:-default}` interpolation, so the file can be committed as is.

| Environment variable | Meaning | Default |
| --- | --- | --- |
| `SCALAR_DB_CONTACT_POINT` | ScalarDB Cluster host | `localhost` |
| `SCALAR_DB_USERNAME` | ScalarDB Cluster user | `scalaradmin` |
| `SCALAR_DB_PASSWORD` | ScalarDB Cluster password | `scalaradmin` |

Point the samples at your cluster by exporting the variables:

```console
export SCALAR_DB_CONTACT_POINT=<your-cluster-host>
export SCALAR_DB_USERNAME=<your-user>
export SCALAR_DB_PASSWORD=<your-password>
```

Alternatively, edit the defaults in `scalardb.properties` directly.

## Running the samples

Create the schema first, then run any of the CRUD samples, and drop the schema when you are done.

```console
./gradlew runCreateTable     # Coordinator tables + namespace + sample table

./gradlew runInsertRecord    # insert  pk=1, ck=2
./gradlew runGetRecord       # get     pk=1, ck=2
./gradlew runUpdateRecord    # update  pk=1, ck=2
./gradlew runUpsertRecord    # upsert  pk=1, ck=2
./gradlew runScanRecord      # scan    pk=1 (up to 10 records)
./gradlew runDeleteRecord    # delete  pk=1, ck=2

./gradlew runDropTable       # sample table + namespace + Coordinator tables
```

`./gradlew tasks --group application` lists all eight tasks.

The record the samples operate on is hard-coded to `pk=1, ck=2` so that each class stays
readable on its own. Edit the constants at the top of `main` to use different values.

## Schema

`CreateTable.java` builds the schema programmatically. `simple.sample.json` is the
equivalent definition for the
[ScalarDB Schema Loader](https://scalardb.scalar-labs.com/docs/latest/schema-loader/),
and the two are kept in sync.

| Column | Type | Role |
| --- | --- | --- |
| `pk` | `INT` | Partition key |
| `ck` | `INT` | Clustering key (ASC) |
| `text_value` | `TEXT` | Secondary index |

## Directory Structure

```
│  README.md
│  build.gradle
│  settings.gradle
│  scalardb.properties      # Configure this file, or set the environment variables above.
│  simple.sample.json       # Schema Loader definition for the sample.
└─src
    └──main
        └──java
            └─com
                └─demo
                    ├─CreateTable.java
                    ├─InsertRecord.java
                    ├─GetRecord.java
                    ├─UpdateRecord.java
                    ├─UpsertRecord.java
                    ├─ScanRecord.java
                    ├─DeleteRecord.java
                    └─DropTable.java
```

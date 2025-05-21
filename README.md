# ScalarDB Cluster - Simple Sample

This is a set of simple examples to demonstrate how to use ScalarDB Cluster with Distribution and Replication.

## Prerequisites

ScalarDB Cluster environment version 3.15.3 or later
JDK 17
Gradle 8.8 or later

## Directory Structure

```bash
│  README.md
│  scalardb.properties      # Please configure this file for your environment.
│  simple.sample.json       # Schema definition for the sample.
└─src
    ├─main
    │  ├─java
    │  │  └─com
    │  │      └─demo
    │  │              CreateTable.java
    │  │              DropTable.java
    │  │              InsertRecord.java
    │  │              GetRecord.java
    │  │              UpdateRecord.java
    │  │              UpsertRecord.java
    │  │              ScanRecord.java
    │  │              DeleteRecord.java
    │  │
    │  └─resources
    └─test
        ├─java
        │  └─com
        │      └─demo
        │              ExcepionTest.java
        │
        └─resources
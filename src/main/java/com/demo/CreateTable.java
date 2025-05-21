package com.demo;

import com.scalar.db.api.*;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import java.util.Optional;

public class CreateTable {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        DistributedTransactionManager manager;
        DistributedTransaction transaction = null;

        try {
            TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
            DistributedTransactionAdmin admin = factory.getTransactionAdmin();

//            Check and Create Coordinator Table
            admin.createCoordinatorTables(true);

//            Check and Create Name Space
            admin.createNamespace(NAME_SPACE_NAME, true);

            TableMetadata  sample =
                    TableMetadata.newBuilder()
                            .addColumn("pk", DataType.INT)
                            .addColumn("ck", DataType.INT)
                            .addColumn("text_value", DataType.TEXT)
                            .addPartitionKey("pk")
                            .addClusteringKey("ck", Scan.Ordering.Order.ASC)
                            .build();

            admin.createTable(NAME_SPACE_NAME, TABLE_NAME, sample);

            System.out.println("Tables were Created");

        } catch (Exception e) {
            if (transaction != null) {
                transaction.abort();
            }
            throw e;
        }
    }
}

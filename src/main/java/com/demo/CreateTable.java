package com.demo;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.service.TransactionFactory;

public class CreateTable {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);

        try (DistributedTransactionAdmin admin = factory.getTransactionAdmin()) {
            // Check and create the Coordinator tables
            admin.createCoordinatorTables(true);

            // Check and create the namespace
            admin.createNamespace(NAME_SPACE_NAME, true);

            TableMetadata sample =
                    TableMetadata.newBuilder()
                            .addColumn("pk", DataType.INT)
                            .addColumn("ck", DataType.INT)
                            .addColumn("text_value", DataType.TEXT)
                            .addPartitionKey("pk")
                            .addClusteringKey("ck", Scan.Ordering.Order.ASC)
                            .addSecondaryIndex("text_value")
                            .build();

            // Check and create the sample table
            admin.createTable(NAME_SPACE_NAME, TABLE_NAME, sample, true);

            System.out.println("Tables were created");
        }
    }
}

package com.demo;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.service.TransactionFactory;

public class DropTable {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);

        try (DistributedTransactionAdmin admin = factory.getTransactionAdmin()) {
            // Check and drop the sample table
            admin.dropTable(NAME_SPACE_NAME, TABLE_NAME, true);

            // Check and drop the namespace
            admin.dropNamespace(NAME_SPACE_NAME, true);

            // Check and drop the Coordinator tables
            admin.dropCoordinatorTables(true);

            System.out.println("Tables were dropped");
        }
    }
}

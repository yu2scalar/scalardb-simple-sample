package com.demo;

import com.scalar.db.api.*;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import java.util.List;
import java.util.Optional;

public class ScanRecord {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        DistributedTransactionManager manager;
        DistributedTransaction transaction = null;

        try {
            Integer pk = 1;

            TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
            manager = factory.getTransactionManager();

            transaction = manager.start();

            List<Result> results = transaction.scan(
                    Scan.newBuilder()
                            .namespace(NAME_SPACE_NAME)
                            .table(TABLE_NAME)
                            .partitionKey(Key.ofInt("pk", pk))
                            .projections("pk", "ck", "text_value")
                            .limit(10)
                            .build());

            transaction.commit();

            if (results.isEmpty()) {
                System.out.println("No record was found.");
            } else {
                for (Result result : results) {
                    System.out.println("-------------------------------------------");
                    System.out.println("pk:" + String.valueOf(result.getInt("pk")));
                    System.out.println("ck:" + String.valueOf(result.getInt("ck")));
                    System.out.println("text_value:" + String.valueOf(result.getText("text_value")));
                }
            }

        } catch (Exception e) {
            if (transaction != null) {
                transaction.abort();
            }
            throw e;
        }
    }
}

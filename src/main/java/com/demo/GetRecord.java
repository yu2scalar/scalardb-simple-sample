package com.demo;

import com.scalar.db.api.*;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import java.util.Optional;

public class GetRecord {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        DistributedTransactionManager manager;
        DistributedTransaction transaction = null;

        try {
            Integer pk = 1;
            Integer ck = 2;

            TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
            manager = factory.getTransactionManager();

            transaction = manager.start();

            Optional<Result> result = transaction.get(
                    Get.newBuilder()
                            .namespace(NAME_SPACE_NAME)
                            .table(TABLE_NAME)
                            .partitionKey(Key.ofInt("pk", pk))
                            .clusteringKey(Key.ofInt("ck", ck))
                            .projections("pk", "ck", "text_value")
                            .build());

            transaction.commit();

            if (result.isEmpty()) {
                System.out.println("No record was found.");
            } else {
                System.out.println("pk:" + String.valueOf(result.get().getInt("pk")));
                System.out.println("ck:" + String.valueOf(result.get().getInt("ck")));
                System.out.println("text_value:" + String.valueOf(result.get().getText("text_value")));
            }

        } catch (Exception e) {
            if (transaction != null) {
                transaction.abort();
            }
            throw e;
        }
    }
}

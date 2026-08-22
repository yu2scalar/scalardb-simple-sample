package com.demo;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import java.util.Optional;

public class GetRecord {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        int pk = 1;
        int ck = 2;

        TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);

        try (DistributedTransactionManager manager = factory.getTransactionManager()) {
            DistributedTransaction transaction = manager.start();

            try {
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
                    System.out.println("pk:" + result.get().getInt("pk"));
                    System.out.println("ck:" + result.get().getInt("ck"));
                    System.out.println("text_value:" + result.get().getText("text_value"));
                }
            } catch (UnknownTransactionStatusException e) {
                // The commit status is unknown, so the transaction must not be rolled back here.
                // Check the state with DistributedTransactionManager#getState instead.
                throw e;
            } catch (Exception e) {
                try {
                    transaction.rollback();
                } catch (Exception rollbackException) {
                    e.addSuppressed(rollbackException);
                }
                throw e;
            }
        }
    }
}

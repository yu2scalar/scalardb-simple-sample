package com.demo;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import java.util.List;

public class ScanRecord {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";
    private static final int SCAN_LIMIT = 10;

    public static void main(String[] args) throws Exception {
        int pk = 1;

        TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);

        try (DistributedTransactionManager manager = factory.getTransactionManager()) {
            DistributedTransaction transaction = manager.start();

            try {
                List<Result> results = transaction.scan(
                        Scan.newBuilder()
                                .namespace(NAME_SPACE_NAME)
                                .table(TABLE_NAME)
                                .partitionKey(Key.ofInt("pk", pk))
                                .projections("pk", "ck", "text_value")
                                .limit(SCAN_LIMIT)
                                .build());

                transaction.commit();

                if (results.isEmpty()) {
                    System.out.println("No record was found.");
                } else {
                    for (Result result : results) {
                        System.out.println("-------------------------------------------");
                        System.out.println("pk:" + result.getInt("pk"));
                        System.out.println("ck:" + result.getInt("ck"));
                        System.out.println("text_value:" + result.getText("text_value"));
                    }
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

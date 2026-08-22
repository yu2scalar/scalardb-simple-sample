package com.demo;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

public class UpdateRecord {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        int pk = 1;
        int ck = 2;
        String textValue = "update-" + pk + "-" + ck;

        TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);

        try (DistributedTransactionManager manager = factory.getTransactionManager()) {
            DistributedTransaction transaction = manager.start();

            try {
                transaction.update(
                        Update.newBuilder()
                                .namespace(NAME_SPACE_NAME)
                                .table(TABLE_NAME)
                                .partitionKey(Key.ofInt("pk", pk))
                                .clusteringKey(Key.ofInt("ck", ck))
                                .textValue("text_value", textValue)
                                .build());

                transaction.commit();

                System.out.println("Record was updated");
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

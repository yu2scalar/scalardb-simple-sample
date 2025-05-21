package com.demo;

import com.scalar.db.api.*;
import com.scalar.db.io.DataType;
import com.scalar.db.service.TransactionFactory;

public class DropTable {

    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";

    public static void main(String[] args) throws Exception {
        DistributedTransactionManager manager;
        DistributedTransaction transaction = null;

        try {
            TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
            DistributedTransactionAdmin admin = factory.getTransactionAdmin();

//            Check and Drop Sample Table
            admin.dropTable(NAME_SPACE_NAME, TABLE_NAME, true);

//            Check and Drop Name Space
            admin.dropNamespace(NAME_SPACE_NAME, true);

        } catch (Exception e) {
            if (transaction != null) {
                transaction.abort();
            }
            throw e;
        }
    }
}

package com.demo;
import com.scalar.db.api.*;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import java.util.Optional;

public class UpdateRecord {

	private static final String NAME_SPACE_NAME = "simple";
	private static final String TABLE_NAME = "sample";
	private static final String SCALARDB_PROPERTIES = "scalardb.properties";

	public static void main(String[] args) throws Exception {
		DistributedTransactionManager manager;
		DistributedTransaction transaction = null;

		try {

			Integer pk = 1;
			Integer ck = 2;
			String textValue = "update" + "-" + pk.toString() + "-" + ck.toString();

			TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
			manager = factory.getTransactionManager();

			transaction = manager.start();
			transaction.update(
					Update.newBuilder()
							.namespace(NAME_SPACE_NAME)
							.table(TABLE_NAME)
							.partitionKey(Key.ofInt("pk", pk))
							.clusteringKey(Key.ofInt("ck", ck))
							.textValue("text_value", textValue).build());
			transaction.commit();

			System.out.println("Records were updated");

		} catch (Exception e) {
        	e.printStackTrace();

			if (transaction != null) {
				transaction.abort();
			}
			throw e;
		}
	}
}

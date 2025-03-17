package com.demo;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Upsert;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;


public class UpsertRecord {

	private static final String NAME_SPACE_NAME = "simple";
	private static final String TABLE_NAME = "sample";
	private static final String SCALARDB_PROPERTIES = "scalardb.properties";

	public static void main(String[] args) throws Exception {
		DistributedTransactionManager manager;
		DistributedTransaction transaction = null;

		Integer pk = 1;
		Integer ck = 1;
		String textValue = "upsert" + "-" + pk.toString() + "-" + ck.toString();

		try {

			TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
			manager = factory.getTransactionManager();

			transaction = manager.start();
			transaction.upsert(
					Upsert.newBuilder()
							.namespace(NAME_SPACE_NAME)
							.table(TABLE_NAME)
							.partitionKey(Key.ofInt("pk", pk))
							.clusteringKey(Key.ofInt("ck", ck))
							.textValue("text_value", textValue).build());
			transaction.commit();
			
    		System.out.println("Records were inserted");

		} catch (Exception e) {
			if (transaction != null) {
				transaction.abort();
			}
			throw e;
		}
	}
}

package com.demo;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;


public class DeleteRecord {

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

			transaction.delete(
					Delete.newBuilder()
							.namespace(NAME_SPACE_NAME)
							.table(TABLE_NAME)
							.partitionKey(Key.ofInt("pk", pk))
							.clusteringKey(Key.ofInt("ck", ck))
							.build());

			transaction.commit();
			
    		System.out.println("Record was deleted");

		} catch (Exception e) {
			if (transaction != null) {
				transaction.abort();
			}
			throw e;
		}
	}
}

package com.demo;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.*;
import com.scalar.db.exception.transaction.*;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

enum TestPattern {
    OK,
    UnsatisfiedConditionException,
    UnknownTransactionStatusException,
    TransactionException,
    TransactionNotFoundException,
    IllegalArgumentException,
    CrudException,
    CrudConflictException,
    CommitConflictException
}

/**
 * com.scalar.db.exception.transaction.CommitException: UNAVAILABLE: no healthy upstream
 */

public class ExcepionTest {

    private DistributedTransactionManager mockManager;
    private DistributedTransaction mockTransaction;
    private static final String SCALARDB_PROPERTIES = "scalardb.properties";
    private static final String SCALARDB_PROPERTIES_CORE = "scalardb_core.properties";
    private static final String SCALARDB_PROPERTIES_WRONG_ADD = "scalardb_wrong_add.properties";
    private static final String SCALARDB_PROPERTIES_WRONG_PASS = "scalardb_wrong_pass.properties";
    private static final String SCALARDB_PROPERTIES_NO_PERMISSION = "scalardb_no_permission.properties";
    private static final String SCALARDB_PROPERTIES_SMALL_GRPC_PRM = "scalardb_small_grpc_size.properties";
    private static final String NAME_SPACE_NAME = "simple";
    private static final String TABLE_NAME = "sample";
    private static final String DUMMY_UUID = "550e8400-e29b-41d4-a716-446655440000";

    private DistributedTransactionManager manager;

    private Enum<TestPattern> testPatternEnum;
    Integer pk = 1;
    Integer ck = 2;
    private String scalardbProperties = "";

    @BeforeEach
    public void setUp() throws Exception {
    }

    @AfterEach
    void tearDown() throws Exception {
        cleanUpRecord();
    }

    private void cleanUpRecord() throws Exception {

        TransactionFactory factory = TransactionFactory.create(scalardbProperties);
        manager = factory.getTransactionManager();

        DistributedTransaction transaction = null;

        transaction = manager.start();

        transaction.delete(
                Delete.newBuilder()
                        .namespace(NAME_SPACE_NAME)
                        .table(TABLE_NAME)
                        .partitionKey(Key.ofInt("pk", pk))
                        .clusteringKey(Key.ofInt("ck", ck))
                        .build());

        transaction.commit();
    }

    private void insertRecord() throws Exception {

        TransactionFactory factory = TransactionFactory.create(scalardbProperties);
        manager = factory.getTransactionManager();

        DistributedTransaction transaction = null;

        transaction = manager.start();

        transaction.insert(
                Insert.newBuilder()
                        .namespace(NAME_SPACE_NAME)
                        .table(TABLE_NAME)
                        .partitionKey(Key.ofInt("pk", pk))
                        .clusteringKey(Key.ofInt("ck", ck))
                        .textValue("text_value", "inserted")
                        .build());

        transaction.commit();
    }

    @Test
    public void testOkTransaction() throws Exception {
        scalardbProperties = SCALARDB_PROPERTIES;
//        String[] args = {SCALARDB_PROPERTIES, "1", ""};
        assertDoesNotThrow(() -> exceptionTest(TestPattern.OK));

    }

    @Test
    public void testUnsatisfiedConditionException() throws Exception {

//        String[] args = {SCALARDB_PROPERTIES, "-1", ""};
        scalardbProperties = SCALARDB_PROPERTIES;

        Exception exception = assertThrows(
                UnsatisfiedConditionException.class,
                () -> exceptionTest(TestPattern.UnsatisfiedConditionException)
        );
        exception.printStackTrace();
    }

//    @Test
//    /**
//     * Failed to reproduce the exception
//     */
//    public void testUnknownTransactionStatusException() throws Exception {
//
////        String[] args = {SCALARDB_PROPERTIES, "1", ""};
//
//        scalardbProperties = SCALARDB_PROPERTIES;
//
//        Exception exception = assertThrows(
//                UnknownTransactionStatusException.class,
//                () -> exceptionTest(TestPattern.UnknownTransactionStatusException)
//        );
//        exception.printStackTrace();
//    }

    @Test
    public void testCommitConflictException() throws Exception {

//        String[] args = {SCALARDB_PROPERTIES, "1", ""};

        scalardbProperties = SCALARDB_PROPERTIES;

        Exception exception = assertThrows(
                CommitConflictException.class,
                () -> exceptionTest(TestPattern.CommitConflictException)
        );
        exception.printStackTrace();
    }


    @Test
    public void testTransactionException() throws Exception {

//        String[] args = {SCALARDB_PROPERTIES_WRONG_ADD, "1", ""};
        scalardbProperties = SCALARDB_PROPERTIES_SMALL_GRPC_PRM;

        Exception exception = assertThrows(
//                TransactionException.class,
                TransactionException.class,
                () -> exceptionTest(TestPattern.TransactionException)
        );
        exception.printStackTrace();
        scalardbProperties = SCALARDB_PROPERTIES;
    }

    @Test
/**
 *  With Cluster environment, the following test throw the TransactionNotFoundException in the middle of the stacktrace.
 *  But the exception became CrudConflictException eventually.
 *  You can reproduce TransactionNotFoundException case with Core lib.
 */
    public void testTransactionNotFoundException() throws Exception {
//        String[] args = {SCALARDB_PROPERTIES, "1", "550e8400-e29b-41d4-a716-446655440000"};
        scalardbProperties = SCALARDB_PROPERTIES_CORE;

        Exception exception = assertThrows(
                TransactionNotFoundException.class,
                () -> exceptionTest(TestPattern.TransactionNotFoundException)
        );
        exception.printStackTrace();

    }

    @Test
    public void testIllegalArgumentException() throws Exception {
//        String[] args = {SCALARDB_PROPERTIES, "0", ""};
//        com.demo.ExcepionInsert.main(SCALARDB_PROPERTIES);
        scalardbProperties = SCALARDB_PROPERTIES;

        Exception exception = assertThrows(
                IllegalArgumentException.class,
                () -> exceptionTest(TestPattern.IllegalArgumentException)
        );
        exception.printStackTrace();

    }

    @Test
    public void testCrudException() throws Exception {
//        String[] args = {SCALARDB_PROPERTIES_NO_PERMISSION, "1", ""};

        scalardbProperties = SCALARDB_PROPERTIES_NO_PERMISSION;

        Exception exception = assertThrows(
                CrudException.class,
                () -> exceptionTest(TestPattern.CrudException)
        );
        exception.printStackTrace();
        scalardbProperties = SCALARDB_PROPERTIES;

    }

    @Test
    public void testCrudConflictException() throws Exception {
//        String[] args = {SCALARDB_PROPERTIES, "1", "550e8400-e29b-41d4-a716-446655440000"};
        scalardbProperties = SCALARDB_PROPERTIES;

        Exception exception = assertThrows(
                CrudConflictException.class,
                () -> exceptionTest(TestPattern.CrudConflictException)
        );
        exception.printStackTrace();

    }

    public void exceptionTest(Enum<TestPattern> testPatternEnum) throws Exception {
//        public void exceptionTest(String[] args) throws Exception {
        DistributedTransactionManager manager;
        DistributedTransaction transaction = null;

//        String prop = args[0];

        String textValue = UUID.randomUUID().toString();
        MutationCondition updateIfExistsCondition = ConditionBuilder.updateIfExists();


        try {

            TransactionFactory factory = TransactionFactory.create(scalardbProperties);
            manager = factory.getTransactionManager();
            if (testPatternEnum == TestPattern.TransactionNotFoundException
                    || testPatternEnum == TestPattern.CrudConflictException) {
                transaction = manager.resume(DUMMY_UUID);
            } else {
                transaction = manager.start();

            }

            if (testPatternEnum == TestPattern.IllegalArgumentException) {
                transaction.update(
                        Update.newBuilder()
                                .namespace(NAME_SPACE_NAME)
                                .table(TABLE_NAME)
                                .partitionKey(Key.ofInt("pk", pk))
//                                .clusteringKey(Key.ofInt("ck", ck))   // Missing CK
                                .textValue("text_value", textValue)
                                .build());
            } else if (testPatternEnum == TestPattern.UnsatisfiedConditionException) {
                transaction.update(
                        Update.newBuilder()
                                .namespace(NAME_SPACE_NAME)
                                .table(TABLE_NAME)
                                .partitionKey(Key.ofInt("pk", pk))
                                .clusteringKey(Key.ofInt("ck", ck))
                                .condition(updateIfExistsCondition)     // cause UnsatisfiedConditionException
                                .textValue("text_value", textValue)
                                .build());
            } else if (testPatternEnum == TestPattern.CommitConflictException) {
                insertRecord(); // Insert a record

                transaction.update(
                        Update.newBuilder()
                                .namespace(NAME_SPACE_NAME)
                                .table(TABLE_NAME)
                                .partitionKey(Key.ofInt("pk", pk))
                                .clusteringKey(Key.ofInt("ck", ck))
                                .textValue("text_value", textValue)
                                .build());

                                exceptionTest(TestPattern.OK);    // update the same record

            } else {
                insertRecord(); // Insert a record

                transaction.update(
                        Update.newBuilder()
                                .namespace(NAME_SPACE_NAME)
                                .table(TABLE_NAME)
                                .partitionKey(Key.ofInt("pk", pk))
                                .clusteringKey(Key.ofInt("ck", ck))
                                .textValue("text_value", textValue)
                                .build());
            }

            transaction.commit();
        } catch (UnsatisfiedConditionException e) {
            throw e;

            // ----------------------------------------
        } catch (TransactionNotFoundException e) {
            throw e;

            // ----------------------------------------
        } catch (UnknownTransactionStatusException e) {
            throw e;

        } catch (CrudConflictException e) {
            throw e;

        } catch (TransactionException e) {
            throw e;


        } catch (RuntimeException e) {
            throw e;

        } catch (Exception e) {
            throw e;

        }

    }

}
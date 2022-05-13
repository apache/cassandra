/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.service.accord.txn.*;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue.Substitution;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.cql3.statements.TransactionStatement.DUPLICATE_TUPLE_NAME_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.EMPTY_TRANSACTION_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.INCOMPLETE_PRIMARY_KEY_LET_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_CONDITIONS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_TIMESTAMPS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.SELECT_REFS_NEED_COLUMN_MESSAGE;
import static org.apache.cassandra.service.accord.txn.TxnDataName.user;
import static org.apache.cassandra.cql3.statements.UpdateStatement.UPDATING_PRIMARY_KEY_MESSAGE;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.cql3.transactions.RowDataReference.CANNOT_FIND_TUPLE_MESSAGE;
import static org.apache.cassandra.cql3.transactions.RowDataReference.COLUMN_NOT_IN_TUPLE_MESSAGE;
import static org.apache.cassandra.schema.TableMetadata.UNDEFINED_COLUMN_NAME_MESSAGE;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class TransactionStatementTest
{
    private static final TableId TABLE1_ID = TableId.fromString("00000000-0000-0000-0000-000000000001");
    private static final TableId TABLE2_ID = TableId.fromString("00000000-0000-0000-0000-000000000002");
    private static final TableId TABLE3_ID = TableId.fromString("00000000-0000-0000-0000-000000000003");
    private static final TableId TABLE4_ID = TableId.fromString("00000000-0000-0000-0000-000000000004");

    private static TableMetadata TABLE1;
    private static TableMetadata TABLE2;
    private static TableMetadata TABLE3;
    private static TableMetadata TABLE4;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl1 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE1_ID),
                                    parse("CREATE TABLE tbl2 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE2_ID),
                                    parse("CREATE TABLE tbl3 (k int PRIMARY KEY, \"with spaces\" int, \"with\"\"quote\" int, \"MiXeD_CaSe\" int)", "ks").id(TABLE3_ID),
                                    parse("CREATE TABLE tbl4 (k int PRIMARY KEY, int_list list<int>)", "ks").id(TABLE4_ID));

        TABLE1 = Schema.instance.getTableMetadata("ks", "tbl1");
        TABLE2 = Schema.instance.getTableMetadata("ks", "tbl2");
        TABLE3 = Schema.instance.getTableMetadata("ks", "tbl3");
        TABLE4 = Schema.instance.getTableMetadata("ks", "tbl4");
    }

    @Test
    public void shouldRejectReferenceSelectOutsideTxn()
    {
        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement("SELECT row1.v, row2.v;"))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("expecting K_FROM");
    }

    @Test
    public void shouldRejectReferenceUpdateOutsideTxn()
    {
        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement("UPDATE ks.tbl1 SET v = row2.v WHERE k=1 AND c=2;"))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectConditionalWithNoEndIf()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectConditionalWithEndIfButNoIf()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectLetOnlyStatement()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(EMPTY_TRANSACTION_MESSAGE);
    }

    @Test
    public void shouldRejectEntireTupleSelect()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  SELECT row1;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(SELECT_REFS_NEED_COLUMN_MESSAGE);
    }

    @Test
    public void shouldRejectDuplicateTupleName()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(DUPLICATE_TUPLE_NAME_MESSAGE, "row1"));
    }

    @Test
    public void shouldRejectIllegalLimitInLet()
    {
        String letSelect = "SELECT * FROM ks.tbl1 WHERE k = 1 LIMIT 2";
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (" + letSelect + ");\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, letSelect));
    }

    @Test
    public void shouldRejectIncompletePrimaryKeyInLet()
    {
        String letSelect = "SELECT * FROM ks.tbl1 WHERE k = 1";
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (" + letSelect + ");\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, letSelect));
    }

    @Test
    public void shouldRejectIllegalLimitInSelect()
    {
        String select = "SELECT * FROM ks.tbl1 WHERE k = 1 LIMIT 2";
        String query = "BEGIN TRANSACTION\n" + select + ";\nCOMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, select));
    }

    @Test
    public void shouldRejectIncompletePrimaryKeyInSelect()
    {
        String select = "SELECT * FROM ks.tbl1 WHERE k = 1";
        String query = "BEGIN TRANSACTION\n" + select + ";\nCOMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, select));
    }

    @Test
    public void shouldRejectUpdateWithCondition()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) IF NOT EXISTS;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_CONDITIONS_IN_UPDATES_MESSAGE);
    }

    @Test
    public void shouldRejectUpdateWithCustomTimestamp()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_TIMESTAMPS_IN_UPDATES_MESSAGE);
    }

    @Test
    public void shouldRejectBothFullSelectAndSelectWithReferences()
    {
        String query = "BEGIN TRANSACTION\n" +
                "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                "  SELECT v FROM ks.tbl1 WHERE k=2 AND c=2;\n" +
                "  SELECT row1.v;\n" +
                "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                "  END IF\n" +
                "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("no viable alternative");
    }

    @Test
    public void shouldRejectPrimaryKeyValueReference()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=1);\n" +
                       "  IF row1 IS NULL THEN\n" +
                       "    UPDATE ks.tbl1 SET c = row1.c WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(UPDATING_PRIMARY_KEY_MESSAGE, "c"));
    }

    @Test
    public void shouldRejectShorthandAssignmentToUnknownColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET q += 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(UNDEFINED_COLUMN_NAME_MESSAGE, "q", "ks.tbl1"));
    }

    @Test
    public void shouldRejectAdditionToUnknownColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET v = q + 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("Only expressions of the form X = X +<value> are supported.");
    }

    @Test
    public void shouldRejectUnknownSubstitutionTuple()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET v = row1.v WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(CANNOT_FIND_TUPLE_MESSAGE, "row1"));
    }

    @Test
    public void shouldRejectUnknownSubstitutionColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  UPDATE ks.tbl1 SET v = row1.q WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(String.format(COLUMN_NOT_IN_TUPLE_MESSAGE, "q", "row1"));
    }

    @Test
    public void simpleQueryTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=?);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=2 AND c=?;\n" +
                       "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=? THEN\n" +
                       "    UPDATE ks.tbl1 SET v=? WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead(TxnDataName.returning(), "SELECT v FROM ks.tbl1 WHERE k=2 AND c=2")
                                 .withWrite("UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2")
                                 .withIsNotNullCondition(user("row1"), null)
                                 .withEqualsCondition("row1", "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        List<ByteBuffer> values = ImmutableList.of(bytes(2), bytes(2), bytes(4), bytes(1));
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.forInternalCalls(values));

        assertEquals(expected, actual);
    }

    @Test
    public void txnWithReturningStatement()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT row1.v, row2.v;\n" +
                       "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withWrite("UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2")
                                 .withIsNotNullCondition(user("row1"), null)
                                 .withEqualsCondition("row1", "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);

        assertEquals(expected, actual);
        assertEquals(2, statement.getReturningReferences().size());
    }

    @Test
    public void testQuotedColumnNames()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl3 WHERE k=1);\n" +
                       "  SELECT \"row1\".\"with spaces\", row1.\"with\"\"quote\", row1.\"MiXeD_CaSe\";\n" +
                       "  IF row1.\"with spaces\" IS NULL THEN\n" +
                       "    INSERT INTO ks.tbl3 (k, \"with spaces\") VALUES (1, 2);\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl3 WHERE k=1")
                                 .withWrite("INSERT INTO ks.tbl3 (k, \"with spaces\") VALUES (1, 2)")
                                 .withIsNullCondition(user("row1"), "ks.tbl3.with spaces")
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);

        assertEquals(expected, actual);
        assertEquals(3, statement.getReturningReferences().size());
        assertEquals("with spaces", statement.getReturningReferences().get(0).column().name.toString());
        assertEquals("with\"quote", statement.getReturningReferences().get(1).column().name.toString());
        assertEquals("MiXeD_CaSe", statement.getReturningReferences().get(2).column().name.toString());
    }

    @Test
    public void updateVariableSubstitutionTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v = row2.v WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        List<TxnReferenceOperation> regularOps = new ArrayList<>();
        regularOps.add(new TxnReferenceOperation(TxnReferenceOperation.Kind.ConstantSetter, column(TABLE1, "v"), null, null, new Substitution(reference(user("row2"), TABLE2, "v"))));
        TxnReferenceOperations referenceOps = new TxnReferenceOperations(TABLE1, Clustering.make(bytes(2)), regularOps, Collections.emptyList());
        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead(TxnDataName.returning(), "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite(emptyUpdate(TABLE1, 1, 2, false), referenceOps)
                                 .withEqualsCondition("row1", "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    @Test
    public void insertVariableSubstitutionTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v=4 THEN\n" +
                       "    INSERT INTO ks.tbl1 (k, c, v) VALUES (1, 2, row2.v);\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        List<TxnReferenceOperation> regularOps = new ArrayList<>();
        regularOps.add(new TxnReferenceOperation(TxnReferenceOperation.Kind.ConstantSetter, column(TABLE1, "v"), null, null, new Substitution(reference(user("row2"), TABLE2, "v"))));
        TxnReferenceOperations referenceOps = new TxnReferenceOperations(TABLE1, Clustering.make(bytes(2)), regularOps, Collections.emptyList());
        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead(TxnDataName.returning(), "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite(emptyUpdate(TABLE1, 1, 2, true), referenceOps)
                                 .withEqualsCondition("row1", "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    @Test
    public void testListCondition()
    {
        String update = "BEGIN TRANSACTION\n" +
                        "  LET row1 = (SELECT * FROM ks.tbl4 WHERE k = ?);\n" +
                        "  SELECT row1.int_list;\n" +
                        "  IF row1.int_list = ? THEN\n" +
                        "    UPDATE ks.tbl4 SET int_list = ? WHERE k = ?;\n" +
                        "  END IF\n" +
                        "COMMIT TRANSACTION";

        ListType<Integer> listType = ListType.getInstance(Int32Type.instance, true);
        List<Integer> initialList = Arrays.asList(1, 2);
        ByteBuffer initialListBytes = listType.getSerializer().serialize(initialList);

        List<Integer> updatedList = Arrays.asList(1, 2, 3);
        ByteBuffer updatedListBytes = listType.getSerializer().serialize(updatedList);

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl4 WHERE k = 0")
                                 .withWrite("UPDATE ks.tbl4 SET int_list = ? WHERE k = 0",
                                            TxnReferenceOperations.empty(),
                                            new VariableSpecifications(Collections.singletonList(null)),
                                            updatedListBytes)
                                 .withEqualsCondition("row1", "ks.tbl4.int_list", initialListBytes)
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(update);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());

        List<ByteBuffer> values = ImmutableList.of(bytes(0), initialListBytes, updatedListBytes, bytes(0));
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.forInternalCalls(values));

        // TODO: Find a better way to test, given list paths are randomly generated and therefore not comparable.
        assertEquals(expected.toString().length(), actual.toString().length());

        assertEquals(1, statement.getReturningReferences().size());
        assertEquals("int_list", statement.getReturningReferences().get(0).column().name.toString());
    }

    @Test
    public void testListSubstitution()
    {
        String update = "BEGIN TRANSACTION\n" +
                        "  LET row1 = (SELECT * FROM ks.tbl4 WHERE k = ?);\n" +
                        "  SELECT row1.int_list;\n" +
                        "  IF row1.int_list = [1, 2] THEN\n" +
                        "    UPDATE ks.tbl4 SET int_list = row1.int_list WHERE k = ?;\n" +
                        "  END IF\n" +
                        "COMMIT TRANSACTION";

        ListType<Integer> listType = ListType.getInstance(Int32Type.instance, true);
        List<Integer> initialList = Arrays.asList(1, 2);
        ByteBuffer initialListBytes = listType.getSerializer().serialize(initialList);

        List<TxnReferenceOperation> partitionOps = new ArrayList<>();
        partitionOps.add(new TxnReferenceOperation(TxnReferenceOperation.Kind.ListSetter, column(TABLE4, "int_list"), null, null, new Substitution(reference(user("row1"), TABLE4, "int_list"))));
        TxnReferenceOperations referenceOps = new TxnReferenceOperations(TABLE4, Clustering.EMPTY, partitionOps, Collections.emptyList());

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl4 WHERE k = 0")
                                 .withWrite(emptyUpdate(TABLE4, 1, Clustering.EMPTY, false), referenceOps)
                                 .withEqualsCondition("row1", "ks.tbl4.int_list", initialListBytes)
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(update);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());

        List<ByteBuffer> values = ImmutableList.of(bytes(0), bytes(1));
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.forInternalCalls(values));

        assertEquals(expected, actual);
        assertEquals(1, statement.getReturningReferences().size());
        assertEquals("int_list", statement.getReturningReferences().get(0).column().name.toString());
    }

    private static PartitionUpdate emptyUpdate(TableMetadata metadata, int k, int c, boolean forInsert)
    {
        return emptyUpdate(metadata, k, new BufferClustering(bytes(c)), forInsert);
    }

    private static PartitionUpdate emptyUpdate(TableMetadata metadata, int k, Clustering<?> c, boolean forInsert)
    {
        DecoratedKey dk = metadata.partitioner.decorateKey(bytes(k));
        RegularAndStaticColumns columns = new RegularAndStaticColumns(Columns.from(metadata.regularColumns()), Columns.NONE);
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, dk, columns, 1);

        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(c);
        if (forInsert)
            row.addPrimaryKeyLivenessInfo(LivenessInfo.create(0, 0));
        builder.add(row.build());

        return builder.build();
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(new ColumnIdentifier(name, true));
    }

    private static TxnReference reference(TxnDataName name, TableMetadata metadata, String column)
    {
        return new TxnReference(name, column(metadata, column), null);
    }
}

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

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ast.Conditional.Is;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.TransactionStatement.DUPLICATE_TUPLE_NAME_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.EMPTY_TRANSACTION_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.ILLEGAL_RANGE_QUERY_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.INCOMPLETE_PARTITION_KEY_SELECT_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_AGGREGATION_IN_TXNS_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_CONDITIONS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_GROUP_BY_IN_TXNS_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_ORDER_BY_IN_TXNS_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_PARTITION_IN_CLAUSE_WITH_LIMIT;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_TIMESTAMPS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_TTLS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.SELECT_REFS_NEED_COLUMN_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE;
import static org.apache.cassandra.cql3.statements.UpdateStatement.CANNOT_SET_KEY_WITH_REFERENCE_MESSAGE;
import static org.apache.cassandra.cql3.statements.UpdateStatement.UPDATING_PRIMARY_KEY_MESSAGE;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.cql3.transactions.RowDataReference.CANNOT_FIND_TUPLE_MESSAGE;
import static org.apache.cassandra.cql3.transactions.RowDataReference.COLUMN_NOT_IN_TUPLE_MESSAGE;
import static org.apache.cassandra.schema.TableMetadata.UNDEFINED_COLUMN_NAME_MESSAGE;

public class TransactionStatementTest
{
    private static final TableId TABLE1_ID = TableId.fromString("00000000-0000-0000-0000-000000000001");
    private static final TableId TABLE2_ID = TableId.fromString("00000000-0000-0000-0000-000000000002");
    private static final TableId TABLE3_ID = TableId.fromString("00000000-0000-0000-0000-000000000003");
    private static final TableId TABLE4_ID = TableId.fromString("00000000-0000-0000-0000-000000000004");
    private static final TableId TABLE5_ID = TableId.fromString("00000000-0000-0000-0000-000000000005");
    private static final TableId TABLE6_ID = TableId.fromString("00000000-0000-0000-0000-000000000006");
    private static final TableId TABLE7_ID = TableId.fromString("00000000-0000-0000-0000-000000000007");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl1 (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'", "ks").id(TABLE1_ID),
                                    parse("CREATE TABLE tbl2 (k int, c int, v int, primary key (k, c)) WITH transactional_mode = 'full'", "ks").id(TABLE2_ID),
                                    parse("CREATE TABLE tbl3 (k int PRIMARY KEY, \"with spaces\" int, \"with\"\"quote\" int, \"MiXeD_CaSe\" int) WITH transactional_mode = 'full'", "ks").id(TABLE3_ID),
                                    parse("CREATE TABLE tbl4 (k int PRIMARY KEY, int_list list<int>) WITH transactional_mode = 'full'", "ks").id(TABLE4_ID),
                                    parse("CREATE TABLE tbl5 (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'", "ks").id(TABLE5_ID),
                                    parse("CREATE TABLE tbl6 (k int PRIMARY KEY, v int) WITH transactional_mode = 'off'", "ks").id(TABLE6_ID),
                                    parse("CREATE TABLE tbl7 (k int PRIMARY KEY, v vector<float, 1>) WITH transactional_mode = 'full'", "ks").id(TABLE7_ID));
    }

    private static TableMetadata tbl(int num)
    {
        return Keyspace.open("ks").getColumnFamilyStore("tbl" + num).metadata();
    }

    private static TableMetadata tbl5()
    {
        return tbl(5);
    }

    @Test
    public void shouldRejectReferenceSelectOutsideTxn()
    {
        String query = "SELECT row1.v, row2.v;";
        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("expecting K_FROM");
    }

    @Test
    public void shouldRejectReferenceUpdateOutsideTxn()
    {
        String query = "UPDATE ks.tbl1 SET v = row2.v WHERE k=1 AND c=2;";
        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectLetOnlyStatement()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "LET assignment row1", "at [2:15]"));
    }

    @Test
    public void shouldRejectIllegalBindLimitInLet()
    {
        String letSelect = "SELECT * FROM ks.tbl1 WHERE k = 1 LIMIT ?";
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (" + letSelect + ");\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> execute(query, 2))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "LET assignment", "at [2:15]"));
    }

    @Test
    public void shouldRejectIncompletePrimaryKeyInLet()
    {
        String letSelect = "SELECT * FROM ks.tbl1 WHERE k = 1";
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (" + letSelect + ");\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "LET assignment row1", "at [2:15]"));
    }

    @Test
    public void shouldRejectUpdateWithCondition()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) IF NOT EXISTS;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_CONDITIONS_IN_UPDATES_MESSAGE, "INSERT", "at [2:3]");
    }

    @Test
    public void shouldRejectUpdateWithCustomTimestamp()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_TIMESTAMPS_IN_UPDATES_MESSAGE, "INSERT", "at [2:3]");
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

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(UPDATING_PRIMARY_KEY_MESSAGE, "c"));
    }

    @Test
    public void shouldRejectShorthandAssignmentToUnknownColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET q += 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
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

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(COLUMN_NOT_IN_TUPLE_MESSAGE, "q", "row1"));
    }

    @Test
    public void shouldRejectInsertPartiitonKeyReference()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row0 = (SELECT * FROM ks.tbl1 WHERE k = 0 AND c = 0);\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (row0.k, 1, 1);\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(CANNOT_SET_KEY_WITH_REFERENCE_MESSAGE, "row0.k", "k"));
    }

    @Test
    public void shouldRejectNormalSelectWithIncompletePartitionKey()
    {
        String select = "SELECT k, v FROM ks.tbl5 LIMIT 1";
        String query = "BEGIN TRANSACTION\n" +
                       select + ";\n" +
                       "COMMIT TRANSACTION;\n";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PARTITION_KEY_SELECT_MESSAGE, "returning select", "at [2:1]"));
    }

    @Test
    public void shouldRejectLetSelectWithIncompletePartitionKey()
    {
        String select = "SELECT k, v FROM ks.tbl5 WHERE token(k) > token(123) LIMIT 1";
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (" + select + "); \n" +
                       "  SELECT row1.k, row1.v;\n" +
                       "COMMIT TRANSACTION;\n";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(ILLEGAL_RANGE_QUERY_MESSAGE, "LET assignment row1", "at [2:15]"));
    }

    @Test
    public void shouldRejectTTL()
    {
        for (Mutation.Kind kind : Mutation.Kind.values())
        {
            if (kind == Mutation.Kind.DELETE) continue; // deletes don't support TTL
            Mutation mutation;
            switch (kind)
            {
                case INSERT:
                    mutation = Mutation.insert(tbl5())
                                       .value("k", 1)
                                       .value("v", 2)
                                       .ttl(42)
                                       .build();
                    break;
                case UPDATE:
                    mutation = Mutation.update(tbl5())
                                       .value("k", 1)
                                       .set("v", 2)
                                       .ttl(42)
                                       .build();
                    break;
                default:
                    throw new UnsupportedOperationException(kind.name());
            }
            String query = Txn.wrap(mutation).toCQL();
            Assertions.assertThatThrownBy(() -> prepare(query))
                      .isInstanceOf(InvalidRequestException.class)
                      .hasMessageContaining(String.format(NO_TTLS_IN_UPDATES_MESSAGE, kind.name(), "at"));

            var txn = Txn.builder()
                         .addLet("a", Select.builder()
                                            .table(tbl5())
                                            .value("k", 1)
                                            .build())
                         .addIf(new Is("a", Is.Kind.Null), mutation)
                         .build();
            Assertions.assertThatThrownBy(() -> prepare(txn.toCQL()))
                      .isInstanceOf(InvalidRequestException.class)
                      .hasMessageContaining(String.format(NO_TTLS_IN_UPDATES_MESSAGE, kind.name(), "at"));
        }
    }

    @Test
    public void shouldRejectAggFunctions()
    {
        var select = Select.builder()
                           .selection(FunctionCall.count("v"))
                           .table(tbl5())
                           .value("k",0)
                           .build();

        Assertions.assertThatThrownBy(() -> prepare(Txn.wrap(select).toCQL()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_AGGREGATION_IN_TXNS_MESSAGE, "SELECT", "at"));

        var txn = Txn.builder()
                     .addLet("a", select)
                     .addReturnReferences("a.count")
                     .build();

        Assertions.assertThatThrownBy(() -> prepare(txn.toCQL()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_AGGREGATION_IN_TXNS_MESSAGE, "SELECT", "at"));
    }

    @Test
    public void shouldRejectOrderBy()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  SELECT * FROM ks.tbl7 WHERE k=0 ORDER BY v ANN OF [42] LIMIT 1;" +
                       "COMMIT TRANSACTION;";
        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_ORDER_BY_IN_TXNS_MESSAGE, "SELECT", "at"));

        // The below code is left commented out as a reminder to think about this case... As of this writing ORDER BY does not parse in a LET clause... if that is ever fixed we should block it right away!
//        String query2 = "BEGIN TRANSACTION\n" +
//                        "  LET a = (SELECT * FROM ks.tbl7 WHERE k=0 ORDER BY v ANN OF [42] LIMIT 1;)" +
//                        "  SELECT a.v" +
//                        "COMMIT TRANSACTION;";
//        Assertions.assertThatThrownBy(() -> prepare(query2))
//                  .isInstanceOf(InvalidRequestException.class)
//                  .hasMessageContaining(String.format(NO_ORDER_BY_IN_TXNS_MESSAGE, "SELECT", "at"));
    }

    @Test
    public void shouldRejectGroupBy()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  SELECT * FROM ks.tbl1 WHERE k=0 GROUP BY c LIMIT 1;" +
                       "COMMIT TRANSACTION;";
        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_GROUP_BY_IN_TXNS_MESSAGE, "SELECT", "at"));

        // The below code is left commented out as a reminder to think about this case... As of this writing GROUP BY does not parse in a LET clause... if that is ever fixed we should block it right away!
//        String query2 = "BEGIN TRANSACTION\n" +
//                        "  LET a = (SELECT * FROM ks.tbl1 WHERE k=0 GROUP BY c LIMIT 1;)" +
//                        "  SELECT a.v" +
//                        "COMMIT TRANSACTION;";
//        Assertions.assertThatThrownBy(() -> prepare(query2))
//                  .isInstanceOf(InvalidRequestException.class)
//                  .hasMessageContaining(String.format(NO_GROUP_BY_IN_TXNS_MESSAGE, "SELECT", "at"));
    }

    @Test
    public void shouldRejectInClauseInLet()
    {
        // this is blocked not because this isn't safe, but that the logic to handle this is currently in the read coordinator, which Accord doesn't call.
        // So rather than return bad results to users, IN w/ LIMIT is blocked... until we can fix
        Select select = Select.builder()
                              .table(tbl(1))
                              .in("k", 0, 1)
                              .limit(Literal.of(1))
                              .build();

        Assertions.assertThatThrownBy(() -> prepare(Txn.wrap(select).toCQL()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", "at"));

        Assertions.assertThatThrownBy(() -> prepare(Txn.builder()
                                                       .addLet("a", select)
                                                       .addReturnReferences("a.k")
                                                       .build().toCQL()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", "at"));
    }

    @Test
    public void shouldRejectInClauseInLetWithBind()
    {
        Select select = Select.builder()
                              .table(tbl(1))
                              .in("k", 0, 1)
                              .limit(1)
                              .build();

        TransactionStatement stmt = (TransactionStatement) prepare(Txn.wrap(select).toCQL());
        QueryState state = QueryState.forInternalCalls();
        Dispatcher.RequestTime now = Dispatcher.RequestTime.forImmediateExecution();
        Assertions.assertThatThrownBy(() -> stmt.execute(state, QueryOptions.forInternalCalls(Arrays.asList(select.bindsEncoded())), now)).isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", "at"));

        Txn txn = Txn.builder()
                   .addLet("a", select)
                   .addReturnReferences("a.v")
                   .build();
        TransactionStatement stmt2 = (TransactionStatement) prepare(txn.toCQL());
        Assertions.assertThatThrownBy(() -> stmt2.execute(state, QueryOptions.forInternalCalls(Arrays.asList(txn.bindsEncoded())), now)).isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", "at"));
    }

    @Test
    public void shouldRejectLetSelectOnNonTransactionalTable()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl6 WHERE k = 0);\n" +
                       "  INSERT INTO ks.tbl5 (k, v) VALUES (1, 2);\n" +
                       "COMMIT TRANSACTION;";

        Assertions.assertThatThrownBy(() -> prepare(query))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(String.format(TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, "SELECT", "at [2:15]"));
    }

    @Test
    public void shouldRejectSelectOnNonTransactionalTable()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  SELECT * FROM ks.tbl6 WHERE k = 0;\n" +
                       "COMMIT TRANSACTION;";

        Assertions.assertThatThrownBy(() -> prepare(query))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(String.format(TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, "SELECT", "at [2:3]"));
    }

    @Test
    public void shouldRejectUpdateOnNonTransactionalTable()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl6 (k, v) VALUES (1, 2);\n" +
                       "COMMIT TRANSACTION;";

        Assertions.assertThatThrownBy(() -> prepare(query))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(String.format(TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, "INSERT", "at [2:3]"));
    }

    @Test
    public void shouldRejectRowReferenceOnLHSExceptIsNullAndIsNotNull()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=1);\n" +
                       "  IF row1 = 1 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=1;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("Row reference (row1) can only be used with IS NULL/IS NOT NULL conditions");
    }

    @Test
    public void shouldRejectRowReferenceOnRHSExceptIsNullAndIsNotNull()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=1);\n" +
                       "  IF 100 = row1 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=1;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("Row reference (row1) can only be used with IS NULL/IS NOT NULL conditions");
    }

    @Test
    public void shouldAcceptRowReferenceWithIsNullAndIsNotNull()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=1);\n" +
                       "  IF row1 IS NULL THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=1;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";
        Assertions.assertThat(prepare(query)).isNotNull();
        query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=1);\n" +
                       "  IF row1 IS NOT NULL THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=1;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";
        Assertions.assertThat(prepare(query)).isNotNull();
    }

    private static CQLStatement prepare(String query)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        return parsed.prepare(ClientState.forInternalCalls());
    }

    private static ResultMessage execute(String query, Object... binds)
    {
        CQLStatement stmt = prepare(query);
        return stmt.execute(QueryState.forInternalCalls(), QueryProcessor.makeInternalOptions(stmt, binds), Dispatcher.RequestTime.forImmediateExecution());
    }
}

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

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.TransactionStatement.DUPLICATE_TUPLE_NAME_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.EMPTY_TRANSACTION_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.INCOMPLETE_PRIMARY_KEY_LET_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_CONDITIONS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.NO_TIMESTAMPS_IN_UPDATES_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.SELECT_REFS_NEED_COLUMN_MESSAGE;
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

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl1 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE1_ID),
                                    parse("CREATE TABLE tbl2 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE2_ID),
                                    parse("CREATE TABLE tbl3 (k int PRIMARY KEY, \"with spaces\" int, \"with\"\"quote\" int, \"MiXeD_CaSe\" int)", "ks").id(TABLE3_ID),
                                    parse("CREATE TABLE tbl4 (k int PRIMARY KEY, int_list list<int>)", "ks").id(TABLE4_ID),
                                    parse("CREATE TABLE tbl5 (k int PRIMARY KEY, v int)", "ks").id(TABLE5_ID));
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
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, letSelect));
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
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, letSelect.replace("?", "2")));
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
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, letSelect));
    }

    @Test
    public void shouldRejectIllegalLimitInSelect()
    {
        String select = "SELECT * FROM ks.tbl1 WHERE k = 1 LIMIT 2";
        String query = "BEGIN TRANSACTION\n" + select + ";\nCOMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, select));
    }

    @Test
    public void shouldRejectIncompletePrimaryKeyInSelect()
    {
        String select = "SELECT * FROM ks.tbl1 WHERE k = 1";
        String query = "BEGIN TRANSACTION\n" + select + ";\nCOMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, select));
    }

    @Test
    public void shouldRejectUpdateWithCondition()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) IF NOT EXISTS;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_CONDITIONS_IN_UPDATES_MESSAGE);
    }

    @Test
    public void shouldRejectUpdateWithCustomTimestamp()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> prepare(query))
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
        String query = "BEGIN TRANSACTION\n" +
                       "  SELECT k, v FROM ks.tbl5 LIMIT 1;\n" +
                       "COMMIT TRANSACTION;\n";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "SELECT v FROM ks.tbl5 LIMIT 1"));
    }

    @Test
    public void shouldRejectLetSelectWithIncompletePartitionKey()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT k, v FROM ks.tbl5 WHERE token(k) > token(123) LIMIT 1); \n" +
                       "  SELECT row1.k, row1.v;\n" +
                       "COMMIT TRANSACTION;\n";

        Assertions.assertThatThrownBy(() -> prepare(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, "SELECT v FROM ks.tbl5 WHERE token(k) > 0000007b LIMIT 1"));
    }

    private static CQLStatement prepare(String query)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        return parsed.prepare(ClientState.forInternalCalls());
    }

    private static ResultMessage execute(String query, Object... binds)
    {
        CQLStatement stmt = prepare(query);
        return stmt.execute(QueryState.forInternalCalls(), QueryProcessor.makeInternalOptions(stmt, binds), 0);
    }
}

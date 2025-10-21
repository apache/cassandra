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

package org.apache.cassandra.cql3.ast;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableMetadata;
import org.assertj.core.api.Assertions;

public class CQLFormatterPrettyPrintTest
{
    private static final TableMetadata TBL1 = TableMetadata.builder("ks", "tbl")
                                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                                           .addClusteringColumn("ck", Int32Type.instance)
                                                           .addRegularColumn("v0", Int32Type.instance)
                                                           .addRegularColumn("v1", Int32Type.instance)
                                                           .build();
    private static final Symbol pk = new Symbol("pk", Int32Type.instance);
    private static final Symbol ck = new Symbol("ck", Int32Type.instance);
    private static final Symbol v0 = new Symbol("v0", Int32Type.instance);
    private static final Symbol v1 = new Symbol("v1", Int32Type.instance);

    @Test
    public void selectTest()
    {
        Select select = select();
        Assertions.assertThat(select.toCQL(format())).isEqualTo("SELECT *\n" +
                                                                "FROM ks.tbl\n" +
                                                                "WHERE foo = ?\n" +
                                                                "LIMIT ?");
    }

    @Test
    public void insertTest()
    {
        Mutation.InsertBuilder builder = Mutation.insert(TBL1);
        builder.value("pk", 0);
        builder.value("v1", 0);
        builder.ttl(Literal.of(42));
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("INSERT INTO ks.tbl (pk, v1)\n" +
                                                                         "VALUES (?, ?)\n" +
                                                                         "USING TTL 42");
    }

    @Test
    public void updateTest()
    {
        Mutation.TableBasedUpdateBuilder builder = Mutation.update(TBL1);
        builder.value("pk", 0);
        builder.set("v0", 0);
        builder.set("v1", 0);
        builder.ttl(Literal.of(42));
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("UPDATE ks.tbl\n" +
                                                                         "USING TTL 42\n" +
                                                                         "SET\n" +
                                                                         "  v0=?,\n" +
                                                                         "  v1=?\n" +
                                                                         "WHERE \n" +
                                                                         "  pk = ?");

    }

    @Test
    public void updateWithInClause()
    {
        Mutation.TableBasedUpdateBuilder builder = Mutation.update(TBL1);
        builder.set("v0", 0);
        builder.in("pk", Bind.of(42), Literal.of(78));
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("UPDATE ks.tbl\n" +
                                                                         "SET\n" +
                                                                         "  v0=?\n" +
                                                                         "WHERE \n" +
                                                                         "  pk IN (?, 78)");
    }

    @Test
    public void updateWithBetweenClause()
    {
        Mutation.TableBasedUpdateBuilder builder = Mutation.update(TBL1);
        builder.set("v0", 0);
        builder.value("pk", 0);
        builder.between("ck", Bind.of(42), Literal.of(78));
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("UPDATE ks.tbl\n" +
                                                                         "SET\n" +
                                                                         "  v0=?\n" +
                                                                         "WHERE \n" +
                                                                         "  pk = ? AND \n" +
                                                                         "  ck BETWEEN ? AND 78");
    }

    @Test
    public void updateWithIsClause()
    {
        Mutation.TableBasedUpdateBuilder builder = Mutation.update(TBL1);
        builder.set("v0", 0);
        builder.value("pk", 0);
        builder.is(new Symbol("ck", Int32Type.instance),
                             Conditional.Is.Kind.Null);
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("UPDATE ks.tbl\n" +
                                                                         "SET\n" +
                                                                         "  v0=?\n" +
                                                                         "WHERE \n" +
                                                                         "  pk = ? AND \n" +
                                                                         "  ck IS NULL");
    }

    @Test
    public void deleteTest()
    {
        Mutation.DeleteBuilder builder = Mutation.delete(TBL1);
        builder.column(v1, v0);
        builder.value("pk", 0);
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("DELETE v1, v0\n" +
                                                                         "FROM ks.tbl\n" +
                                                                         "WHERE \n" +
                                                                         "  pk = ?");
    }

    @Test
    public void txnTest()
    {
        Txn.Builder builder = Txn.builder();
        builder.addLet("a", select());
        builder.addLet("b", select());
        builder.addReturn(select());
        Assertions.assertThat(builder.build().toCQL(format())).isEqualTo("BEGIN TRANSACTION\n" +
                                                                         "  LET a = (SELECT *\n" +
                                                                         "           FROM ks.tbl\n" +
                                                                         "           WHERE foo = ?\n" +
                                                                         "           LIMIT ?);\n" +
                                                                         "  LET b = (SELECT *\n" +
                                                                         "           FROM ks.tbl\n" +
                                                                         "           WHERE foo = ?\n" +
                                                                         "           LIMIT ?);\n" +
                                                                         "  SELECT *\n" +
                                                                         "  FROM ks.tbl\n" +
                                                                         "  WHERE foo = ?\n" +
                                                                         "  LIMIT ?;\n" +
                                                                         "COMMIT TRANSACTION");
    }

    private static CQLFormatter.PrettyPrint format()
    {
        return new CQLFormatter.PrettyPrint();
    }

    private static Select select()
    {
        return Select.builder()
                     .table("ks", "tbl")
                     .value("foo",42)
                     .limit(1)
                     .build();
    }
}
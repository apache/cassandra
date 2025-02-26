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

package org.apache.cassandra.contraints;


import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Generators;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static accord.utils.Property.qt;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.integers;

@RunWith(Parameterized.class)
public class CreateTableWithColumnCqlConstraintValidationTest extends CqlConstraintValidationTester
{

    @Parameterized.Parameter
    public String order;

    @Parameterized.Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
        { "ASC" },
        { "DESC" }
        });
    }

    @Test
    public void testCreateTableWithColumnNotNamedConstraintDescribeTableNonFunction() throws Throwable
    {
        String table = createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 100, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE_PER_TEST + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int CHECK ck1 < 100,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE_PER_TEST + "." + table),
                      row(KEYSPACE_PER_TEST,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableWithColumnMultipleConstraintsDescribeTableNonFunction() throws Throwable
    {
        String table = createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 100 AND ck1 > 10, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE_PER_TEST + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int CHECK ck1 < 100 AND ck1 > 10,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE_PER_TEST + "." + table),
                      row(KEYSPACE_PER_TEST,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableWithColumnNotNamedConstraintDescribeTableFunction() throws Throwable
    {
        String table = createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE_PER_TEST + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 text CHECK LENGTH(ck1) = 4,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE_PER_TEST + "." + table),
                      row(KEYSPACE_PER_TEST,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableWithColumnNotNullConstraintDescribe() throws Throwable
    {
        String table = createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int CHECK NOT_NULL(v), PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE_PER_TEST + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 int,\n" +
                                      "    v int CHECK NOT_NULL(v),\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE_PER_TEST + "." + table),
                      row(KEYSPACE_PER_TEST,
                          "table",
                          table,
                          tableCreateStatement));
    }

    // SCALAR
    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessThanScalarConstraintInteger() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerThanScalarConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(5, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(0, 4)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 4)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(5, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnDifferentThanScalarConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));
        qt().forAll(Generators.toGen(integers().between(5, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 4, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnMultipleScalarConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 4 AND ck1 >= 2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(2, 3)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(-100, 1)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });

        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4";
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessThanScalarSmallIntConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 smallint CHECK ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerThanScalarSmallIntConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 smallint CHECK ck1 > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(5, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));


        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(0, 4)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarSmallIntConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 smallint CHECK ck1 >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarSmallIntConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 smallint CHECK ck1 <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 4)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(5, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnDifferentThanScalarSmallIntConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 smallint CHECK ck1 != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));
        qt().forAll(Generators.toGen(integers().between(5, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 4, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnMultipleScalarSmallIntConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 smallint CHECK ck1 < 4 AND ck1 >= 2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(integers().between(2, 3)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2";
        // Invalid
        qt().forAll(Generators.toGen(integers().between(-100, 1)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });

        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4";
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessThanScalarDecimalConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 decimal CHECK ck1 < 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 2, 3)");
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));


        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerThanScalarDecimalConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 decimal CHECK ck1 > 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(0, 4.2)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarDecimalConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 decimal CHECK ck1 >= 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(4.2, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarDecimalConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 decimal CHECK ck1 <= 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(0, 4.2)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnDifferentThanScalarDecimalConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 decimal CHECK ck1 != 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4.2";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 4.2, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnMultipleScalarDecimalConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 decimal CHECK ck1 < 4.2 AND ck1 >= 2.1, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(2.1, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2.1";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(-100, 2)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });

        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2";
        qt().forAll(Generators.toGen(doubles().between(4.2, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessThanScalarDoubleConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 double CHECK ck1 < 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 2, 3)");
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerThanScalarDoubleConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 double CHECK ck1 > 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(0, 4.2)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarDoubleConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 double CHECK ck1 >= 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(4.2, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarDoubleConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 double CHECK ck1 <= 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(0, 4.2)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnDifferentThanScalarDoubleConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 double CHECK ck1 != 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4.2";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 4.2, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnMultipleScalarDoubleConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 double CHECK ck1 < 4.2 AND ck1 >= 2.1, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(2.1, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2.1";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(-100, 2)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });

        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2";
        qt().forAll(Generators.toGen(doubles().between(4.2, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessThanScalarFloatConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 float CHECK ck1 < 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 2, 3)");
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerThanScalarFloatConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 float CHECK ck1 > 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(0, 4.2)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarFloatConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 float CHECK ck1 >= 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(4.2, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarFloatConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 float CHECK ck1 <= 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(0, 4.2)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4.2";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnDifferentThanScalarFloatConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 float CHECK ck1 != 4.2, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(0, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));
        qt().forAll(Generators.toGen(doubles().between(4.3, 100)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4.2";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 4.2, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnMultipleScalarFloatConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 float CHECK ck1 < 4.2 AND ck1 >= 2.1, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        qt().forAll(Generators.toGen(doubles().between(2.1, 4.1)))
            .check(d -> execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2.1";
        // Invalid
        qt().forAll(Generators.toGen(doubles().between(-100, 2)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });

        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2";
        qt().forAll(Generators.toGen(doubles().between(4.2, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testCreateTableWithColumnWithNotNullCheckScalarIntConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int CHECK v < 4 AND v >= 2, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithNotNullCheckScalarSmallintConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v smallint CHECK v < 4 AND v >= 2, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithNotNullCheckScalarDecimalConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v decimal CHECK v < 4 AND v >= 2, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithNotNullCheckScalarDoubleConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v double CHECK v < 4 AND v >= 2, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithNotNullCheckScalarFloatConstraints() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v float CHECK v < 4 AND v >= 2, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    // FUNCTION
    @Test
    public void testCreateTableWithColumnWithClusteringColumnLengthEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) = 4, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLengthDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");

        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLengthBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLengthBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLengthSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnLengthSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK LENGTH(ck1) <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnLengthEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK LENGTH(ck1) = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fooo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foo'), 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foooo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnLengthDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK LENGTH(ck1) != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foo'), 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foooo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fooo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnLengthBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK LENGTH(ck1) > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foooo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foo'), 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fooo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnLengthBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK LENGTH(ck1) >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foooo'), 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fooo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnLengthSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK LENGTH(ck1) < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fooo'), 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foooo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnLengthSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK LENGTH(ck1) <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foo'), 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fooo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('foooo'), 2, 3)");
    }


    @Test
    public void testCreateTableWithColumnWithPkColumnLengthEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) = 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 1, 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnLengthDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) != 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 1, 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnLengthBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) > 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 1, 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnLengthBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) >= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 1, 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnLengthSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) < 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 1, 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnLengthSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) <= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 1, 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 1, 2, 3)");
    }


    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fooo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foooo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) != 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foo')");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foooo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fooo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) > 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foooo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fooo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) >= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foooo')");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fooo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) < 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fooo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foooo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foo')");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fooo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'foooo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthCheckNullTextConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK LENGTH(v) <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthCheckNullVarcharConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v varchar CHECK LENGTH(v) <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthCheckNullAsciiConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v ascii CHECK LENGTH(v) <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnLengthCheckNullBlobConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v blob CHECK LENGTH(v) <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnMixedColumnsLengthConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK LENGTH(pk) = 4, ck1 int, ck2 int, v text CHECK LENGTH(v) = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 2, 3, 'fooo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 2, 3, 'foo')");
        assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 2, 3, 'foo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 2, 3, 'fooo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 2, 3, 'fooo')");
        assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 2, 3, 'foooo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 2, 3, 'foooo')");
    }

    @Test
    public void testCreateTableWithWrongColumnConstraint() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk text, ck1 int CHECK LENGTH(pk) = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
            fail();
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
            assertTrue(e.getMessage().contains("Error setting schema for test"));
        }
    }

    @Test
    public void testCreateTableWithWrongColumnMultipleConstraint() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk text, ck1 int CHECK LENGTH(pk) = 4 AND ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
            fail();
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
            assertTrue(e.getMessage().contains("Error setting schema for test"));
        }
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnInvalidTypeConstraint() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk int, ck1 int CHECK LENGTH(ck1) = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
            fail();
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
            assertTrue(e.getMessage().contains("Error setting schema for test"));
        }
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnInvalidScalarTypeConstraint() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk text CHECK pk = 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
            fail();
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
            assertTrue(e.getCause().getMessage().contains("can be used only for columns of type"));
            assertTrue(e.getMessage().contains("Error setting schema for test"));
        }
    }

    @Test
    public void testCreateTableInvalidFunction() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk text CHECK not_a_function(pk) = 4, ck1 int, ck2 int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
            fail();
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
            assertTrue(e.getMessage().contains("Error setting schema for test"));
        }
    }

    @Test
    public void testCreateTableWithPKConstraintsAndCDCEnabled() throws Throwable
    {
        // It works
        createTable("CREATE TABLE %s (pk text CHECK length(pk) = 4, ck1 int, ck2 int, PRIMARY KEY ((pk), ck1, ck2)) WITH cdc = true;");
    }

    @Test
    public void testCreateTableWithClusteringConstraintsAndCDCEnabled() throws Throwable
    {
        // It works
        createTable("CREATE TABLE %s (pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY ((pk), ck1, ck2)) WITH cdc = true;");
    }

    @Test
    public void testCreateTableWithRegularConstraintsAndCDCEnabled() throws Throwable
    {
        // It works
        createTable("CREATE TABLE %s (pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY (pk)) WITH cdc = true;");
    }

    // Copy table with like
    @Test
    public void testCreateTableWithColumnWithClusteringColumnLessThanScalarConstraintIntegerOnLikeTable() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");", "liketabletame");

        execute("create table " + KEYSPACE + ".tb_copy like %s");

        // Valid
        qt().forAll(Generators.toGen(integers().between(0, 3)))
            .check(d -> execute("INSERT INTO " + KEYSPACE + ".tb_copy (pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)"));

        // Invalid
        qt().forAll(Generators.toGen(integers().between(4, 100)))
            .check(d -> {
                try
                {
                    assertInvalidThrow(InvalidRequestException.class, "INSERT INTO " + KEYSPACE + ".tb_copy(pk, ck1, ck2, v) VALUES (1, " + d + ", 3, 4)");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
    }
}

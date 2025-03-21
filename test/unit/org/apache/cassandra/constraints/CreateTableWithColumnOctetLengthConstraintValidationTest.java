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

package org.apache.cassandra.constraints;


import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.quicktheories.generators.SourceDSL.integers;

@RunWith(Parameterized.class)
public class CreateTableWithColumnOctetLengthConstraintValidationTest extends CqlConstraintValidationTester
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
    public void testCreateTableWithColumnWithClusteringColumnSerializedSizeEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooo', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fño', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foo', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fooñ', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'foooo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnSerializedSizeDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK OCTET_LENGTH() != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñ', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñoo', 2, 3)");

        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fño', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnSerializedSizeBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK OCTET_LENGTH() > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñoo', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñ', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fño', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnSerializedSizeBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK OCTET_LENGTH() >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñoo', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fño', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñ', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnSerializedSizeSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK OCTET_LENGTH() < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñ', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fño', 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñoo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringColumnSerializedSizeSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text CHECK OCTET_LENGTH() <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñ', 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fño', 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 'fñoo', 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fño'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñ'), 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñoo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK OCTET_LENGTH() != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñ'), 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñoo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fño'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK OCTET_LENGTH() > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñoo'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñ'), 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fño'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK OCTET_LENGTH() >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñoo'), 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fño'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñ'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK OCTET_LENGTH() < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñ'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fño'), 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñoo'), 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 blob CHECK OCTET_LENGTH() <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñ'), 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fño'), 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, textAsBlob('fñoo'), 2, 3)");
    }


    @Test
    public void testCreateTableWithColumnWithPkColumnSerializedSizeEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() = 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 1, 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnSerializedSizeDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() != 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 1, 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnSerializedSizeBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() > 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 1, 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnSerializedSizeBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() >= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 1, 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnSerializedSizeSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() < 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 1, 2, 3)");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 1, 2, 3)");
    }

    @Test
    public void testCreateTableWithColumnWithPkColumnSerializedSizeSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() <= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 1, 2, 3)");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 1, 2, 3)");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 1, 2, 3)");
    }


    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeEqualToConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fño')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñ')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñoo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeDifferentThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() != 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñ')");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñoo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fño')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeBiggerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() > 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñoo')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñ')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fño')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeBiggerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() >= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñoo')");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fño')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñ')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeSmallerThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() < 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñ')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fño')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñoo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeSmallerOrEqualThanConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñ')");
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fño')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, 'fñoo')");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullTextConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullVarcharConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v varchar CHECK OCTET_LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullAsciiConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v ascii CHECK OCTET_LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullBlobConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v blob CHECK OCTET_LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null.";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)");
    }

    @Test
    public void testCreateTableWithColumnMixedColumnsSerializedSizeConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK OCTET_LENGTH() = 4, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");

        // Valid
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 2, 3, 'fño')");

        final String expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of";
        final String expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'v'. It has a length of";
        // Invalid
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 2, 3, 'fñ')");
        assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 2, 3, 'fñ')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 2, 3, 'fño')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 2, 3, 'fño')");
        assertInvalidThrowMessage(expectedErrorMessage2, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 2, 3, 'fñoo')");
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 2, 3, 'fñoo')");
    }

    @Test
    public void testCreateTableWithWrongColumnConstraint() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk text, ck1 int CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
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
            createTable("CREATE TABLE %s (pk text, ck1 int CHECK OCTET_LENGTH() = 4 AND ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
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
            createTable("CREATE TABLE %s (pk int, ck1 int CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
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
            assertTrue(e.getCause().getMessage().contains("Constraint 'pk =' can be used only for columns of type"));
            assertTrue(e.getMessage().contains("Error setting schema for test"));
        }
    }

    @Test
    public void testCreateTableInvalidFunction() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (pk text CHECK not_a_function() = 4, ck1 int, ck2 int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 " + order + ");");
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
        createTable("CREATE TABLE %s (pk text CHECK length() = 4, ck1 int, ck2 int, PRIMARY KEY ((pk), ck1, ck2)) WITH cdc = true;");
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

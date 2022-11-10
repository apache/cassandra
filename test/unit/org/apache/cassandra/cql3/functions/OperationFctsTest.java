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
package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.OperationExecutionException;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public class OperationFctsTest extends CQLTester
{

    @Test
    public void testStringConcatenation() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b ascii, c text, PRIMARY KEY(a, b, c))");
        execute("INSERT INTO %S (a, b, c) VALUES ('जॉन', 'Doe', 'जॉन Doe')");

        assertColumnNames(execute("SELECT a + a, a + b, b + a, b + b FROM %s WHERE a = 'जॉन' AND b = 'Doe' AND c = 'जॉन Doe'"),
                "a + a", "a + b", "b + a", "b + b");

        assertRows(execute("SELECT a + ' ' + a, a + ' ' + b, b + ' ' + a, b + ' ' + b FROM %s WHERE a = 'जॉन' AND b = 'Doe' AND c = 'जॉन Doe'"),
                row("जॉन जॉन", "जॉन Doe", "Doe जॉन", "Doe Doe"));
    }

    @Test
    public void testSingleOperations() throws Throwable
    {
        createTable("CREATE TABLE %s (a tinyint, b smallint, c int, d bigint, e float, f double, g varint, h decimal, PRIMARY KEY(a, b, c))");
        execute("INSERT INTO %S (a, b, c, d, e, f, g, h) VALUES (1, 2, 3, 4, 5.5, 6.5, 7, 8.5)");

        // Test additions
        assertColumnNames(execute("SELECT a + a, b + a, c + a, d + a, e + a, f + a, g + a, h + a FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                          "a + a", "b + a", "c + a", "d + a", "e + a", "f + a", "g + a", "h + a");

        assertRows(execute("SELECT a + a, b + a, c + a, d + a, e + a, f + a, g + a, h + a FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row((byte) 2, (short) 3, 4, 5L, 6.5F, 7.5, BigInteger.valueOf(8), BigDecimal.valueOf(9.5)));

        assertRows(execute("SELECT a + b, b + b, c + b, d + b, e + b, f + b, g + b, h + b FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row((short) 3, (short) 4, 5, 6L, 7.5F, 8.5, BigInteger.valueOf(9), BigDecimal.valueOf(10.5)));

        assertRows(execute("SELECT a + c, b + c, c + c, d + c, e + c, f + c, g + c, h + c FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(4, 5, 6, 7L, 8.5F, 9.5, BigInteger.valueOf(10), BigDecimal.valueOf(11.5)));

        assertRows(execute("SELECT a + d, b + d, c + d, d + d, e + d, f + d, g + d, h + d FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(5L, 6L, 7L, 8L, 9.5, 10.5, BigInteger.valueOf(11), BigDecimal.valueOf(12.5)));

        assertRows(execute("SELECT a + e, b + e, c + e, d + e, e + e, f + e, g + e, h + e FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(6.5F, 7.5F, 8.5F, 9.5, 11.0F, 12.0, BigDecimal.valueOf(12.5), BigDecimal.valueOf(14.0)));

        assertRows(execute("SELECT a + f, b + f, c + f, d + f, e + f, f + f, g + f, h + f FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(7.5, 8.5, 9.5, 10.5, 12.0, 13.0, BigDecimal.valueOf(13.5), BigDecimal.valueOf(15.0)));

        assertRows(execute("SELECT a + g, b + g, c + g, d + g, e + g, f + g, g + g, h + g FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(BigInteger.valueOf(8),
                       BigInteger.valueOf(9),
                       BigInteger.valueOf(10),
                       BigInteger.valueOf(11),
                       BigDecimal.valueOf(12.5),
                       BigDecimal.valueOf(13.5),
                       BigInteger.valueOf(14),
                       BigDecimal.valueOf(15.5)));

        assertRows(execute("SELECT a + h, b + h, c + h, d + h, e + h, f + h, g + h, h + h FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(BigDecimal.valueOf(9.5),
                       BigDecimal.valueOf(10.5),
                       BigDecimal.valueOf(11.5),
                       BigDecimal.valueOf(12.5),
                       BigDecimal.valueOf(14.0),
                       BigDecimal.valueOf(15.0),
                       BigDecimal.valueOf(15.5),
                       BigDecimal.valueOf(17.0)));

        // Test substractions

        assertColumnNames(execute("SELECT a - a, b - a, c - a, d - a, e - a, f - a, g - a, h - a FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                          "a - a", "b - a", "c - a", "d - a", "e - a", "f - a", "g - a", "h - a");

        assertRows(execute("SELECT a - a, b - a, c - a, d - a, e - a, f - a, g - a, h - a FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row((byte) 0, (short) 1, 2, 3L, 4.5F, 5.5, BigInteger.valueOf(6), BigDecimal.valueOf(7.5)));

        assertRows(execute("SELECT a - b, b - b, c - b, d - b, e - b, f - b, g - b, h - b FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row((short) -1, (short) 0, 1, 2L, 3.5F, 4.5, BigInteger.valueOf(5), BigDecimal.valueOf(6.5)));

        assertRows(execute("SELECT a - c, b - c, c - c, d - c, e - c, f - c, g - c, h - c FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-2, -1, 0, 1L, 2.5F, 3.5, BigInteger.valueOf(4), BigDecimal.valueOf(5.5)));

        assertRows(execute("SELECT a - d, b - d, c - d, d - d, e - d, f - d, g - d, h - d FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-3L, -2L, -1L, 0L, 1.5, 2.5, BigInteger.valueOf(3), BigDecimal.valueOf(4.5)));

        assertRows(execute("SELECT a - e, b - e, c - e, d - e, e - e, f - e, g - e, h - e FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-4.5F, -3.5F, -2.5F, -1.5, 0.0F, 1.0, BigDecimal.valueOf(1.5), BigDecimal.valueOf(3.0)));

        assertRows(execute("SELECT a - f, b - f, c - f, d - f, e - f, f - f, g - f, h - f FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-5.5, -4.5, -3.5, -2.5, -1.0, 0.0, BigDecimal.valueOf(0.5), BigDecimal.valueOf(2.0)));

        assertRows(execute("SELECT a - g, b - g, c - g, d - g, e - g, f - g, g - g, h - g FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(BigInteger.valueOf(-6),
                       BigInteger.valueOf(-5),
                       BigInteger.valueOf(-4),
                       BigInteger.valueOf(-3),
                       BigDecimal.valueOf(-1.5),
                       BigDecimal.valueOf(-0.5),
                       BigInteger.valueOf(0),
                       BigDecimal.valueOf(1.5)));

        assertRows(execute("SELECT a - h, b - h, c - h, d - h, e - h, f - h, g - h, h - h FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(BigDecimal.valueOf(-7.5),
                       BigDecimal.valueOf(-6.5),
                       BigDecimal.valueOf(-5.5),
                       BigDecimal.valueOf(-4.5),
                       BigDecimal.valueOf(-3.0),
                       BigDecimal.valueOf(-2.0),
                       BigDecimal.valueOf(-1.5),
                       BigDecimal.valueOf(0.0)));

        // Test multiplications

        assertColumnNames(execute("SELECT a * a, b * a, c * a, d * a, e * a, f * a, g * a, h * a FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                          "a * a", "b * a", "c * a", "d * a", "e * a", "f * a", "g * a", "h * a");

        assertRows(execute("SELECT a * a, b * a, c * a, d * a, e * a, f * a, g * a, h * a FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row((byte) 1, (short) 2, 3, 4L, 5.5F, 6.5, BigInteger.valueOf(7), new BigDecimal("8.50")));

        assertRows(execute("SELECT a * b, b * b, c * b, d * b, e * b, f * b, g * b, h * b FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row((short) 2, (short) 4, 6, 8L, 11.0F, 13.0, BigInteger.valueOf(14), new BigDecimal("17.00")));

        assertRows(execute("SELECT a * c, b * c, c * c, d * c, e * c, f * c, g * c, h * c FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(3, 6, 9, 12L, 16.5F, 19.5, BigInteger.valueOf(21), new BigDecimal("25.50")));

        assertRows(execute("SELECT a * d, b * d, c * d, d * d, e * d, f * d, g * d, h * d FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(4L, 8L, 12L, 16L, 22.0, 26.0, BigInteger.valueOf(28), new BigDecimal("34.00")));

        assertRows(execute("SELECT a * e, b * e, c * e, d * e, e * e, f * e, g * e, h * e FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(5.5F, 11.0F, 16.5F, 22.0, 30.25F, 35.75, new BigDecimal("38.5"), new BigDecimal("46.75")));

        assertRows(execute("SELECT a * f, b * f, c * f, d * f, e * f, f * f, g * f, h * f FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(6.5, 13.0, 19.5, 26.0, 35.75, 42.25, new BigDecimal("45.5"), BigDecimal.valueOf(55.25)));

        assertRows(execute("SELECT a * g, b * g, c * g, d * g, e * g, f * g, g * g, h * g FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(BigInteger.valueOf(7),
                       BigInteger.valueOf(14),
                       BigInteger.valueOf(21),
                       BigInteger.valueOf(28),
                       new BigDecimal("38.5"),
                       new BigDecimal("45.5"),
                       BigInteger.valueOf(49),
                       new BigDecimal("59.5")));

        assertRows(execute("SELECT a * h, b * h, c * h, d * h, e * h, f * h, g * h, h * h FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(new BigDecimal("8.50"),
                       new BigDecimal("17.00"),
                       new BigDecimal("25.50"),
                       new BigDecimal("34.00"),
                       new BigDecimal("46.75"),
                       new BigDecimal("55.25"),
                       new BigDecimal("59.5"),
                       new BigDecimal("72.25")));

        // Test divisions

        assertColumnNames(execute("SELECT a / a, b / a, c / a, d / a, e / a, f / a, g / a, h / a FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                          "a / a", "b / a", "c / a", "d / a", "e / a", "f / a", "g / a", "h / a");

        assertRows(execute("SELECT a / a, b / a, c / a, d / a, e / a, f / a, g / a, h / a FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row((byte) 1, (short) 2, 3, 4L, 5.5F, 6.5, BigInteger.valueOf(7), new BigDecimal("8.5")));

        assertRows(execute("SELECT a / b, b / b, c / b, d / b, e / b, f / b, g / b, h / b FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row((short) 0, (short) 1, 1, 2L, 2.75F, 3.25, BigInteger.valueOf(3), new BigDecimal("4.25")));

        assertRows(execute("SELECT a / c, b / c, c / c, d / c, e / c, f / c, g / c, h / c FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0, 0, 1, 1L, 1.8333334F, 2.1666666666666665, BigInteger.valueOf(2), new BigDecimal("2.83333333333333333333333333333333")));

        assertRows(execute("SELECT a / d, b / d, c / d, d / d, e / d, f / d, g / d, h / d FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0L, 0L, 0L, 1L, 1.375, 1.625, BigInteger.valueOf(1), new BigDecimal("2.125")));

        assertRows(execute("SELECT a / e, b / e, c / e, d / e, e / e, f / e, g / e, h / e FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0.18181819F, 0.36363637F, 0.54545456F, 0.7272727272727273, 1.0F, 1.1818181818181819, new BigDecimal("1.27272727272727272727272727272727"), new BigDecimal("1.54545454545454545454545454545455")));

        assertRows(execute("SELECT a / f, b / f, c / f, d / f, e / f, f / f, g / f, h / f FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0.15384615384615385, 0.3076923076923077, 0.46153846153846156, 0.6153846153846154, 0.8461538461538461, 1.0, new BigDecimal("1.07692307692307692307692307692308"), new BigDecimal("1.30769230769230769230769230769231")));

        assertRows(execute("SELECT a / g, b / g, c / g, d / g, e / g, f / g, g / g, h / g FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(BigInteger.valueOf(0),
                       BigInteger.valueOf(0),
                       BigInteger.valueOf(0),
                       BigInteger.valueOf(0),
                       new BigDecimal("0.78571428571428571428571428571429"),
                       new BigDecimal("0.92857142857142857142857142857143"),
                       BigInteger.valueOf(1),
                       new BigDecimal("1.21428571428571428571428571428571")));

        assertRows(execute("SELECT a / h, b / h, c / h, d / h, e / h, f / h, g / h, h / h FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(new BigDecimal("0.11764705882352941176470588235294"),
                       new BigDecimal("0.23529411764705882352941176470588"),
                       new BigDecimal("0.35294117647058823529411764705882"),
                       new BigDecimal("0.47058823529411764705882352941176"),
                       new BigDecimal("0.64705882352941176470588235294118"),
                       new BigDecimal("0.76470588235294117647058823529412"),
                       new BigDecimal("0.82352941176470588235294117647059"),
                       new BigDecimal("1")));

        // Test modulo operations

        assertColumnNames(execute("SELECT a %% a, b %% a, c %% a, d %% a, e %% a, f %% a, g %% a, h %% a FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                          "a % a", "b % a", "c % a", "d % a", "e % a", "f % a", "g % a", "h % a");

        assertRows(execute("SELECT a %% a, b %% a, c %% a, d %% a, e %% a, f %% a, g %% a, h %% a FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row((byte) 0, (short) 0, 0, 0L, 0.5F, 0.5, BigInteger.valueOf(0), new BigDecimal("0.5")));

        assertRows(execute("SELECT a %% b, b %% b, c %% b, d %% b, e %% b, f %% b, g %% b, h %% b FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row((short) 1, (short) 0, 1, 0L, 1.5F, 0.5, BigInteger.valueOf(1), new BigDecimal("0.5")));

        assertRows(execute("SELECT a %% c, b %% c, c %% c, d %% c, e %% c, f %% c, g %% c, h %% c FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row(1, 2, 0, 1L, 2.5F, 0.5, BigInteger.valueOf(1), new BigDecimal("2.5")));

        assertRows(execute("SELECT a %% d, b %% d, c %% d, d %% d, e %% d, f %% d, g %% d, h %% d FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row(1L, 2L, 3L, 0L, 1.5, 2.5, BigInteger.valueOf(3), new BigDecimal("0.5")));

        assertRows(execute("SELECT a %% e, b %% e, c %% e, d %% e, e %% e, f %% e, g %% e, h %% e FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row(1.0F, 2.0F, 3.0F, 4.0, 0.0F, 1.0, new BigDecimal("1.5"), new BigDecimal("3.0")));

        assertRows(execute("SELECT a %% f, b %% f, c %% f, d %% f, e %% f, f %% f, g %% f, h %% f FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row(1.0, 2.0, 3.0, 4.0, 5.5, 0.0, new BigDecimal("0.5"), new BigDecimal("2.0")));

        assertRows(execute("SELECT a %% g, b %% g, c %% g, d %% g, e %% g, f %% g, g %% g, h %% g FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row(BigInteger.valueOf(1),
                       BigInteger.valueOf(2),
                       BigInteger.valueOf(3),
                       BigInteger.valueOf(4),
                       new BigDecimal("5.5"),
                       new BigDecimal("6.5"),
                       BigInteger.valueOf(0),
                       new BigDecimal("1.5")));

        assertRows(execute("SELECT a %% h, b %% h, c %% h, d %% h, e %% h, f %% h, g %% h, h %% h FROM %s WHERE a = 1 AND b = 2 AND c = 23 %% 5"),
                   row(new BigDecimal("1.0"),
                       new BigDecimal("2.0"),
                       new BigDecimal("3.0"),
                       new BigDecimal("4.0"),
                       new BigDecimal("5.5"),
                       new BigDecimal("6.5"),
                       new BigDecimal("7"),
                       new BigDecimal("0.0")));

        // Test negation

        assertColumnNames(execute("SELECT -a, -b, -c, -d, -e, -f, -g, -h FROM %s WHERE a = 1 AND b = 2"),
                          "-a", "-b", "-c", "-d", "-e", "-f", "-g", "-h");

        assertRows(execute("SELECT -a, -b, -c, -d, -e, -f, -g, -h FROM %s WHERE a = 1 AND b = 2"),
                   row((byte) -1, (short) -2, -3, -4L, -5.5F, -6.5, BigInteger.valueOf(-7), new BigDecimal("-8.5")));

        // Test with null
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", null, (byte) 1, (short) 2, 3);
        assertRows(execute("SELECT a + d, b + d, c + d, d + d, e + d, f + d, g + d, h + d FROM %s WHERE a = 1 AND b = 2"),
                   row(null, null, null, null, null, null, null, null));
    }

    @Test
    public void testModuloWithDecimals() throws Throwable
    {
        createTable("CREATE TABLE %s (numerator decimal, dec_mod decimal, int_mod int, bigint_mod bigint, PRIMARY KEY((numerator, dec_mod)))");
        execute("INSERT INTO %s (numerator, dec_mod, int_mod, bigint_mod) VALUES (123456789112345678921234567893123456, 2, 2, 2)");

        assertRows(execute("SELECT numerator %% dec_mod, numerator %% int_mod, numerator %% bigint_mod from %s"),
                   row(new BigDecimal("0"), new BigDecimal("0.0"), new BigDecimal("0.0")));
    }

    @Test
    public void testSingleOperationsWithLiterals() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 tinyint, c2 smallint, v text, PRIMARY KEY(pk, c1, c2))");
        execute("INSERT INTO %S (pk, c1, c2, v) VALUES (2, 2, 2, 'test')");

        // There is only one function outputing tinyint
        assertRows(execute("SELECT * FROM %s WHERE pk = 2 AND c1 = 1 + 1"),
                   row(2, (byte) 2, (short) 2, "test"));

        // As the operation can only be a sum between tinyints the expected type is tinyint
        assertInvalidMessage("Expected 1 byte for a tinyint (4)",
                             "SELECT * FROM %s WHERE pk = 2 AND c1 = 1 + ?", 1);

        assertRows(execute("SELECT * FROM %s WHERE pk = 2 AND c1 = 1 + ?", (byte) 1),
                   row(2, (byte) 2, (short) 2, "test"));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 + 1 AND c1 = 2"),
                   row(2, (byte) 2, (short) 2, "test"));

        assertRows(execute("SELECT * FROM %s WHERE pk = 2 AND c1 = 2 AND c2 = 1 + 1"),
                   row(2, (byte) 2, (short) 2, "test"));

        assertRows(execute("SELECT * FROM %s WHERE pk = 2 AND c1 = 2 AND c2 = 1 * (1 + 1)"),
                   row(2, (byte) 2, (short) 2, "test"));

        // tinyint, smallint and int could be used there so we need to disambiguate
        assertInvalidMessage("Ambiguous '+' operation with args ? and 1: use type hint to disambiguate, example '(int) ?'",
                             "SELECT * FROM %s WHERE pk = ? + 1 AND c1 = 2", 1);

        assertInvalidMessage("Ambiguous '+' operation with args ? and 1: use type hint to disambiguate, example '(int) ?'",
                             "SELECT * FROM %s WHERE pk = 2 AND c1 = 2 AND c2 = 1 * (? + 1)", 1);

        assertRows(execute("SELECT 1 + 1, v FROM %s WHERE pk = 2 AND c1 = 2"),
                   row(2, "test"));

        // As the output type is unknown the ? type cannot be determined
        assertInvalidMessage("Ambiguous '+' operation with args 1 and ?: use type hint to disambiguate, example '(int) ?'",
                             "SELECT 1 + ?, v FROM %s WHERE pk = 2 AND c1 = 2", 1);

        // As the prefered type for the constants is int, the returned type will be int
        assertRows(execute("SELECT 100 + 50, v FROM %s WHERE pk = 2 AND c1 = 2"),
                   row(150, "test"));

        // As the output type is unknown the ? type cannot be determined
        assertInvalidMessage("Ambiguous '+' operation with args ? and 50: use type hint to disambiguate, example '(int) ?'",
                             "SELECT ? + 50, v FROM %s WHERE pk = 2 AND c1 = 2", 100);

        createTable("CREATE TABLE %s (a tinyint, b smallint, c int, d bigint, e float, f double, g varint, h decimal, PRIMARY KEY(a, b))"
                + " WITH CLUSTERING ORDER BY (b DESC)"); // Make sure we test with ReversedTypes
        execute("INSERT INTO %S (a, b, c, d, e, f, g, h) VALUES (1, 2, 3, 4, 5.5, 6.5, 7, 8.5)");

        // Test additions
        assertColumnNames(execute("SELECT a + 1, b + 1, c + 1, d + 1, e + 1, f + 1, g + 1, h + 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a + 1", "b + 1", "c + 1", "d + 1", "e + 1", "f + 1", "g + 1", "h + 1");

        assertRows(execute("SELECT a + 1, b + 1, c + 1, d + 1, e + 1, f + 1, g + 1, h + 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(2, 3, 4, 5L, 6.5F, 7.5, BigInteger.valueOf(8), BigDecimal.valueOf(9.5)));

        assertRows(execute("SELECT 2 + a, 2 + b, 2 + c, 2 + d, 2 + e, 2 + f, 2 + g, 2 + h FROM %s WHERE a = 1 AND b = 2"),
                   row(3, 4, 5, 6L, 7.5F, 8.5, BigInteger.valueOf(9), BigDecimal.valueOf(10.5)));

        long bigInt = Integer.MAX_VALUE + 10L;

        assertRows(execute("SELECT a + " + bigInt + ","
                               + " b + " + bigInt + ","
                               + " c + " + bigInt + ","
                               + " d + " + bigInt + ","
                               + " e + " + bigInt + ","
                               + " f + " + bigInt + ","
                               + " g + " + bigInt + ","
                               + " h + " + bigInt + " FROM %s WHERE a = 1 AND b = 2"),
                   row(1L + bigInt,
                       2L + bigInt,
                       3L + bigInt,
                       4L + bigInt,
                       5.5 + bigInt,
                       6.5 + bigInt,
                       BigInteger.valueOf(bigInt + 7),
                       BigDecimal.valueOf(bigInt + 8.5)));

        assertRows(execute("SELECT a + 5.5, b + 5.5, c + 5.5, d + 5.5, e + 5.5, f + 5.5, g + 5.5, h + 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(6.5, 7.5, 8.5, 9.5, 11.0, 12.0, BigDecimal.valueOf(12.5), BigDecimal.valueOf(14.0)));

        assertRows(execute("SELECT a + 6.5, b + 6.5, c + 6.5, d + 6.5, e + 6.5, f + 6.5, g + 6.5, h + 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(7.5, 8.5, 9.5, 10.5, 12.0, 13.0, BigDecimal.valueOf(13.5), BigDecimal.valueOf(15.0)));

        // Test substractions

        assertColumnNames(execute("SELECT a - 1, b - 1, c - 1, d - 1, e - 1, f - 1, g - 1, h - 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a - 1", "b - 1", "c - 1", "d - 1", "e - 1", "f - 1", "g - 1", "h - 1");

        assertRows(execute("SELECT a - 1, b - 1, c - 1, d - 1, e - 1, f - 1, g - 1, h - 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 1, 2, 3L, 4.5F, 5.5, BigInteger.valueOf(6), BigDecimal.valueOf(7.5)));

        assertRows(execute("SELECT a - 2, b - 2, c - 2, d - 2, e - 2, f - 2, g - 2, h - 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(-1, 0, 1, 2L, 3.5F, 4.5, BigInteger.valueOf(5), BigDecimal.valueOf(6.5)));

        assertRows(execute("SELECT a - 3, b - 3, 3 - 3, d - 3, e - 3, f - 3, g - 3, h - 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(-2, -1, 0, 1L, 2.5F, 3.5, BigInteger.valueOf(4), BigDecimal.valueOf(5.5)));

        assertRows(execute("SELECT a - " + bigInt + ","
                               + " b - " + bigInt + ","
                               + " c - " + bigInt + ","
                               + " d - " + bigInt + ","
                               + " e - " + bigInt + ","
                               + " f - " + bigInt + ","
                               + " g - " + bigInt + ","
                               + " h - " + bigInt + " FROM %s WHERE a = 1 AND b = 2"),
                   row(1L - bigInt,
                       2L - bigInt,
                       3L - bigInt,
                       4L - bigInt,
                       5.5 - bigInt,
                       6.5 - bigInt,
                       BigInteger.valueOf(7 - bigInt),
                       BigDecimal.valueOf(8.5 - bigInt)));

        assertRows(execute("SELECT a - 5.5, b - 5.5, c - 5.5, d - 5.5, e - 5.5, f - 5.5, g - 5.5, h - 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(-4.5, -3.5, -2.5, -1.5, 0.0, 1.0, BigDecimal.valueOf(1.5), BigDecimal.valueOf(3.0)));

        assertRows(execute("SELECT a - 6.5, b - 6.5, c - 6.5, d - 6.5, e - 6.5, f - 6.5, g - 6.5, h - 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(-5.5, -4.5, -3.5, -2.5, -1.0, 0.0, BigDecimal.valueOf(0.5), BigDecimal.valueOf(2.0)));

        // Test multiplications

        assertColumnNames(execute("SELECT a * 1, b * 1, c * 1, d * 1, e * 1, f * 1, g * 1, h * 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a * 1", "b * 1", "c * 1", "d * 1", "e * 1", "f * 1", "g * 1", "h * 1");

        assertRows(execute("SELECT a * 1, b * 1, c * 1, d * 1, e * 1, f * 1, g * 1, h * 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 3, 4L, 5.5F, 6.5, BigInteger.valueOf(7), new BigDecimal("8.50")));

        assertRows(execute("SELECT a * 2, b * 2, c * 2, d * 2, e * 2, f * 2, g * 2, h * 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(2, 4, 6, 8L, 11.0F, 13.0, BigInteger.valueOf(14), new BigDecimal("17.00")));

        assertRows(execute("SELECT a * 3, b * 3, c * 3, d * 3, e * 3, f * 3, g * 3, h * 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(3, 6, 9, 12L, 16.5F, 19.5, BigInteger.valueOf(21), new BigDecimal("25.50")));

        assertRows(execute("SELECT a * " + bigInt + ","
                            + " b * " + bigInt + ","
                            + " c * " + bigInt + ","
                            + " d * " + bigInt + ","
                            + " e * " + bigInt + ","
                            + " f * " + bigInt + ","
                            + " g * " + bigInt + ","
                            + " h * " + bigInt + " FROM %s WHERE a = 1 AND b = 2"),
                               row(1L * bigInt,
                                   2L * bigInt,
                                   3L * bigInt,
                                   4L * bigInt,
                                   5.5 * bigInt,
                                   6.5 * bigInt,
                                   BigInteger.valueOf(7 * bigInt),
                                   BigDecimal.valueOf(8.5 * bigInt)));

        assertRows(execute("SELECT a * 5.5, b * 5.5, c * 5.5, d * 5.5, e * 5.5, f * 5.5, g * 5.5, h * 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(5.5, 11.0, 16.5, 22.0, 30.25, 35.75, new BigDecimal("38.5"), new BigDecimal("46.75")));

        assertRows(execute("SELECT a * 6.5, b * 6.5, c * 6.5, d * 6.5, e * 6.5, 6.5 * f, g * 6.5, h * 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(6.5, 13.0, 19.5, 26.0, 35.75, 42.25, new BigDecimal("45.5"), BigDecimal.valueOf(55.25)));

        // Test divisions

        assertColumnNames(execute("SELECT a / 1, b / 1, c / 1, d / 1, e / 1, f / 1, g / 1, h / 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a / 1", "b / 1", "c / 1", "d / 1", "e / 1", "f / 1", "g / 1", "h / 1");

        assertRows(execute("SELECT a / 1, b / 1, c / 1, d / 1, e / 1, f / 1, g / 1, h / 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 3, 4L, 5.5F, 6.5, BigInteger.valueOf(7), new BigDecimal("8.5")));

        assertRows(execute("SELECT a / 2, b / 2, c / 2, d / 2, e / 2, f / 2, g / 2, h / 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 1, 1, 2L, 2.75F, 3.25, BigInteger.valueOf(3), new BigDecimal("4.25")));

        assertRows(execute("SELECT a / 3, b / 3, c / 3, d / 3, e / 3, f / 3, g / 3, h / 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 0, 1, 1L, 1.8333334F, 2.1666666666666665, BigInteger.valueOf(2), new BigDecimal("2.83333333333333333333333333333333")));

        assertRows(execute("SELECT a / " + bigInt + ","
                + " b / " + bigInt + ","
                + " c / " + bigInt + ","
                + " d / " + bigInt + ","
                + " e / " + bigInt + ","
                + " f / " + bigInt + ","
                + " g / " + bigInt + " FROM %s WHERE a = 1 AND b = 2"),
                   row(1L / bigInt,
                       2L / bigInt,
                       3L / bigInt,
                       4L / bigInt,
                       5.5 / bigInt,
                       6.5 / bigInt,
                       BigInteger.valueOf(7).divide(BigInteger.valueOf(bigInt))));

        assertRows(execute("SELECT a / 5.5, b / 5.5, c / 5.5, d / 5.5, e / 5.5, f / 5.5, g / 5.5, h / 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(0.18181818181818182, 0.36363636363636365, 0.5454545454545454, 0.7272727272727273, 1.0, 1.1818181818181819, new BigDecimal("1.27272727272727272727272727272727"), new BigDecimal("1.54545454545454545454545454545455")));

        assertRows(execute("SELECT a / 6.5, b / 6.5, c / 6.5, d / 6.5, e / 6.5, f / 6.5, g / 6.5, h / 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(0.15384615384615385, 0.3076923076923077, 0.46153846153846156, 0.6153846153846154, 0.8461538461538461, 1.0, new BigDecimal("1.07692307692307692307692307692308"), new BigDecimal("1.30769230769230769230769230769231")));

        // Test modulo operations

        assertColumnNames(execute("SELECT a %% 1, b %% 1, c %% 1, d %% 1, e %% 1, f %% 1, g %% 1, h %% 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a % 1", "b % 1", "c % 1", "d % 1", "e % 1", "f % 1", "g % 1", "h % 1");

        assertRows(execute("SELECT a %% 1, b %% 1, c %% 1, d %% 1, e %% 1, f %% 1, g %% 1, h %% 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 0, 0, 0L, 0.5F, 0.5, BigInteger.valueOf(0), new BigDecimal("0.5")));

        assertRows(execute("SELECT a %% 2, b %% 2, c %% 2, d %% 2, e %% 2, f %% 2, g %% 2, h %% 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 0, 1, 0L, 1.5F, 0.5, BigInteger.valueOf(1), new BigDecimal("0.5")));

        assertRows(execute("SELECT a %% 3, b %% 3, c %% 3, d %% 3, e %% 3, f %% 3, g %% 3, h %% 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 0, 1L, 2.5F, 0.5, BigInteger.valueOf(1), new BigDecimal("2.5")));

        assertRows(execute("SELECT a %% " + bigInt + ","
                            + " b %% " + bigInt + ","
                            + " c %% " + bigInt + ","
                            + " d %% " + bigInt + ","
                            + " e %% " + bigInt + ","
                            + " f %% " + bigInt + ","
                            + " g %% " + bigInt + ","
                            + " h %% " + bigInt + " FROM %s WHERE a = 1 AND b = 2"),
                               row(1L % bigInt,
                                   2L % bigInt,
                                   3L % bigInt,
                                   4L % bigInt,
                                   5.5 % bigInt,
                                   6.5 % bigInt,
                                   BigInteger.valueOf(7 % bigInt),
                                   BigDecimal.valueOf(8.5 % bigInt)));

        assertRows(execute("SELECT a %% 5.5, b %% 5.5, c %% 5.5, d %% 5.5, e %% 5.5, f %% 5.5, g %% 5.5, h %% 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(1.0, 2.0, 3.0, 4.0, 0.0, 1.0, new BigDecimal("1.5"), new BigDecimal("3.0")));

        assertRows(execute("SELECT a %% 6.5, b %% 6.5, c %% 6.5, d %% 6.5, e %% 6.5, f %% 6.5, g %% 6.5, h %% 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(1.0, 2.0, 3.0, 4.0, 5.5, 0.0, new BigDecimal("0.5"), new BigDecimal("2.0")));

        assertRows(execute("SELECT a, b, 1 + 1, 2 - 1, 2 * 2, 2 / 1 , 2 %% 1, (int) -1 FROM %s WHERE a = 1 AND b = 2"),
                   row((byte) 1, (short) 2, 2, 1, 4, 2, 0, -1));
    }

    @Test
    public void testDivisionWithDecimals() throws Throwable
    {
        createTable("CREATE TABLE %s (numerator decimal, denominator decimal, PRIMARY KEY((numerator, denominator)))");
        execute("INSERT INTO %s (numerator, denominator) VALUES (8.5, 200000000000000000000000000000000000)");
        execute("INSERT INTO %s (numerator, denominator) VALUES (10000, 3)");

        assertRows(execute("SELECT numerator / denominator from %s"),
                   row(new BigDecimal("0.0000000000000000000000000000000000425")),
                   row(new BigDecimal("3333.33333333333333333333333333333333")));
    }

    @Test
    public void testWithCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b counter)");
        execute("UPDATE %s SET b = b + 1 WHERE a = 1");
        execute("UPDATE %s SET b = b + 1 WHERE a = 1");
        assertRows(execute("SELECT b FROM %s WHERE a = 1"), row(2L));

        assertRows(execute("SELECT b + (tinyint) 1,"
                + " b + (smallint) 1,"
                + " b + 1,"
                + " b + (bigint) 1,"
                + " b + (float) 1.5,"
                + " b + 1.5,"
                + " b + (varint) 1,"
                + " b + (decimal) 1.5,"
                + " b + b FROM %s WHERE a = 1"),
                   row(3L, 3L, 3L, 3L, 3.5, 3.5, BigInteger.valueOf(3), new BigDecimal("3.5"), 4L));

        assertRows(execute("SELECT b - (tinyint) 1,"
                + " b - (smallint) 1,"
                + " b - 1,"
                + " b - (bigint) 1,"
                + " b - (float) 1.5,"
                + " b - 1.5,"
                + " b - (varint) 1,"
                + " b - (decimal) 1.5,"
                + " b - b FROM %s WHERE a = 1"),
                   row(1L, 1L, 1L, 1L, 0.5, 0.5, BigInteger.valueOf(1), new BigDecimal("0.5"), 0L));

        assertRows(execute("SELECT b * (tinyint) 1,"
                + " b * (smallint) 1,"
                + " b * 1,"
                + " b * (bigint) 1,"
                + " b * (float) 1.5,"
                + " b * 1.5,"
                + " b * (varint) 1,"
                + " b * (decimal) 1.5,"
                + " b * b FROM %s WHERE a = 1"),
                   row(2L, 2L, 2L, 2L, 3.0, 3.0, BigInteger.valueOf(2), new BigDecimal("3.00"), 4L));

        assertRows(execute("SELECT b / (tinyint) 1,"
                + " b / (smallint) 1,"
                + " b / 1,"
                + " b / (bigint) 1,"
                + " b / (float) 0.5,"
                + " b / 0.5,"
                + " b / (varint) 1,"
                + " b / (decimal) 0.5,"
                + " b / b FROM %s WHERE a = 1"),
                   row(2L, 2L, 2L, 2L, 4.0, 4.0, BigInteger.valueOf(2), new BigDecimal("4"), 1L));

        assertRows(execute("SELECT b %% (tinyint) 1,"
                + " b %% (smallint) 1,"
                + " b %% 1,"
                + " b %% (bigint) 1,"
                + " b %% (float) 0.5,"
                + " b %% 0.5,"
                + " b %% (varint) 1,"
                + " b %% (decimal) 0.5,"
                + " b %% b FROM %s WHERE a = 1"),
                   row(0L, 0L, 0L, 0L, 0.0, 0.0, BigInteger.valueOf(0), new BigDecimal("0.0"), 0L));

        assertRows(execute("SELECT -b FROM %s WHERE a = 1"), row(-2L));
    }

    @Test
    public void testPrecedenceAndParentheses() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY(a, b))");
        execute("INSERT INTO %S (a, b, c, d) VALUES (2, 5, 25, 4)");

        UntypedResultSet rs = execute("SELECT a - c / b + d FROM %s");
        assertColumnNames(rs, "a - c / b + d");
        assertRows(rs, row(1));

        rs = execute("SELECT (c - b) / a + d FROM %s");
        assertColumnNames(rs, "(c - b) / a + d");
        assertRows(rs, row(14));

        rs = execute("SELECT c / a / b FROM %s");
        assertColumnNames(rs, "c / a / b");
        assertRows(rs, row(2));

        rs = execute("SELECT c / b / d FROM %s");
        assertColumnNames(rs, "c / b / d");
        assertRows(rs, row(1));

        rs = execute("SELECT (c - a) %% d / a FROM %s");
        assertColumnNames(rs, "(c - a) % d / a");
        assertRows(rs, row(1));

        rs = execute("SELECT (c - a) %% d / a + d FROM %s");
        assertColumnNames(rs, "(c - a) % d / a + d");
        assertRows(rs, row(5));

        rs = execute("SELECT -(c - a) %% d / a + d FROM %s");
        assertColumnNames(rs, "-(c - a) % d / a + d");
        assertRows(rs, row(3));

        rs = execute("SELECT (-c - a) %% d / a + d FROM %s");
        assertColumnNames(rs, "(-c - a) % d / a + d");
        assertRows(rs, row(3));

        rs = execute("SELECT c - a %% d / a + d FROM %s");
        assertColumnNames(rs, "c - a % d / a + d");
        assertRows(rs, row(28));

        rs = execute("SELECT (int)((c - a) %% d / (a + d)) FROM %s");
        assertColumnNames(rs, "(int)((c - a) % d / (a + d))");
        assertRows(rs, row(0));

        // test with aliases
        rs = execute("SELECT (int)((c - a) %% d / (a + d)) as result FROM %s");
        assertColumnNames(rs, "result");
        assertRows(rs, row(0));

        rs = execute("SELECT c / a / b as divisions FROM %s");
        assertColumnNames(rs, "divisions");
        assertRows(rs, row(2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = (int) ? / 2 - 5", 2, 20),
                   row(2, 5, 25, 4));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = (int) ? / (2 + 2)", 2, 20),
                   row(2, 5, 25, 4));
    }

    @Test
    public void testWithDivisionByZero() throws Throwable
    {
        createTable("CREATE TABLE %s (a tinyint, b smallint, c int, d bigint, e float, f double, g varint, h decimal, PRIMARY KEY(a, b))");
        execute("INSERT INTO %S (a, b, c, d, e, f, g, h) VALUES (0, 2, 3, 4, 5.5, 6.5, 7, 8.5)");

        assertInvalidThrowMessage("the operation 'tinyint / tinyint' failed: / by zero",
                                  OperationExecutionException.class,
                                  "SELECT a / a FROM %s WHERE a = 0 AND b = 2");

        assertInvalidThrowMessage("the operation 'smallint / tinyint' failed: / by zero",
                                  OperationExecutionException.class,
                                  "SELECT b / a FROM %s WHERE a = 0 AND b = 2");

        assertInvalidThrowMessage("the operation 'int / tinyint' failed: / by zero",
                                  OperationExecutionException.class,
                                  "SELECT c / a FROM %s WHERE a = 0 AND b = 2");

        assertInvalidThrowMessage("the operation 'bigint / tinyint' failed: / by zero",
                                  OperationExecutionException.class,
                                  "SELECT d / a FROM %s WHERE a = 0 AND b = 2");

        assertInvalidThrowMessage("the operation 'smallint / smallint' failed: / by zero",
                                  OperationExecutionException.class,
                                  "SELECT a FROM %s WHERE a = 0 AND b = 10/0");

        assertRows(execute("SELECT e / a FROM %s WHERE a = 0 AND b = 2"), row(Float.POSITIVE_INFINITY));
        assertRows(execute("SELECT f / a FROM %s WHERE a = 0 AND b = 2"), row(Double.POSITIVE_INFINITY));

        assertInvalidThrowMessage("the operation 'varint / tinyint' failed: BigInteger divide by zero",
                                  OperationExecutionException.class,
                                  "SELECT g / a FROM %s WHERE a = 0 AND b = 2");

        assertInvalidThrowMessage("the operation 'decimal / tinyint' failed: BigInteger divide by zero",
                                  OperationExecutionException.class,
                                  "SELECT h / a FROM %s WHERE a = 0 AND b = 2");
    }

    @Test
    public void testWithNanAndInfinity() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b double, c decimal)");
        assertInvalidMessage("Ambiguous '+' operation with args ? and 1: use type hint to disambiguate, example '(int) ?'",
                             "INSERT INTO %S (a, b, c) VALUES (? + 1, ?, ?)", 0, Double.NaN, BigDecimal.valueOf(1));

        execute("INSERT INTO %S (a, b, c) VALUES ((int) ? + 1, -?, ?)", 0, Double.NaN, BigDecimal.valueOf(1));

        assertRows(execute("SELECT * FROM %s"), row(1, Double.NaN, BigDecimal.valueOf(1)));

        assertRows(execute("SELECT a + NAN, b + 1 FROM %s"), row(Double.NaN, Double.NaN));
        assertInvalidThrowMessage("the operation 'decimal + double' failed: A NaN cannot be converted into a decimal",
                                  OperationExecutionException.class,
                                  "SELECT c + NAN FROM %s");

        assertRows(execute("SELECT a + (float) NAN, b + 1 FROM %s"), row(Float.NaN, Double.NaN));
        assertInvalidThrowMessage("the operation 'decimal + float' failed: A NaN cannot be converted into a decimal",
                                  OperationExecutionException.class,
                                  "SELECT c + (float) NAN FROM %s");

        execute("INSERT INTO %S (a, b, c) VALUES (?, ?, ?)", 1, Double.POSITIVE_INFINITY, BigDecimal.valueOf(1));
        assertRows(execute("SELECT * FROM %s"), row(1, Double.POSITIVE_INFINITY, BigDecimal.valueOf(1)));

        assertRows(execute("SELECT a + INFINITY, b + 1 FROM %s"), row(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        assertInvalidThrowMessage("the operation 'decimal + double' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException.class,
                                  "SELECT c + INFINITY FROM %s");

        assertRows(execute("SELECT a + (float) INFINITY, b + 1 FROM %s"), row(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        assertInvalidThrowMessage("the operation 'decimal + float' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException.class,
                                  "SELECT c + (float) INFINITY FROM %s");

        execute("INSERT INTO %S (a, b, c) VALUES (?, ?, ?)", 1, Double.NEGATIVE_INFINITY, BigDecimal.valueOf(1));
        assertRows(execute("SELECT * FROM %s"), row(1, Double.NEGATIVE_INFINITY, BigDecimal.valueOf(1)));

        assertRows(execute("SELECT a + -INFINITY, b + 1 FROM %s"), row(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        assertInvalidThrowMessage("the operation 'decimal + double' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException.class,
                                  "SELECT c + -INFINITY FROM %s");

        assertRows(execute("SELECT a + (float) -INFINITY, b + 1 FROM %s"), row(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        assertInvalidThrowMessage("the operation 'decimal + float' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException.class,
                                  "SELECT c + (float) -INFINITY FROM %s");
    }

    @Test
    public void testInvalidTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b boolean, c text)");
        execute("INSERT INTO %S (a, b, c) VALUES (?, ?, ?)", 1, true, "test");

        assertInvalidMessage("the '+' operation is not supported between a and b", "SELECT a + b FROM %s");
        assertInvalidMessage("the '+' operation is not supported between b and c", "SELECT b + c FROM %s");
        assertInvalidMessage("the '+' operation is not supported between b and 1", "SELECT b + 1 FROM %s");
        assertInvalidMessage("the '+' operation is not supported between 1 and b", "SELECT 1 + b FROM %s");
        assertInvalidMessage("the '+' operation is not supported between b and NaN", "SELECT b + NaN FROM %s");
        assertInvalidMessage("the '/' operation is not supported between a and b", "SELECT a / b FROM %s");
        assertInvalidMessage("the '/' operation is not supported between b and c", "SELECT b / c FROM %s");
        assertInvalidMessage("the '/' operation is not supported between b and 1", "SELECT b / 1 FROM %s");
        assertInvalidMessage("the '/' operation is not supported between NaN and b", "SELECT NaN / b FROM %s");
        assertInvalidMessage("the '/' operation is not supported between -Infinity and b", "SELECT -Infinity / b FROM %s");
    }

    @Test
    public void testOverflow() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b tinyint, c smallint)");
        execute("INSERT INTO %S (a, b, c) VALUES (?, ?, ?)", 1, (byte) 1, (short) 1);
        assertRows(execute("SELECT a + (int) ?, b + (tinyint) ?, c + (smallint) ? FROM %s", 1, (byte) 1, (short) 1),
                   row(2, (byte) 2,(short) 2));
        assertRows(execute("SELECT a + (int) ?, b + (tinyint) ?, c + (smallint) ? FROM %s", Integer.MAX_VALUE, Byte.MAX_VALUE, Short.MAX_VALUE),
                   row(Integer.MIN_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE));
    }

    @Test
    public void testOperationsWithDuration() throws Throwable
    {
        // Test with timestamp type.
        createTable("CREATE TABLE %s (pk int, time timestamp, v int, primary key (pk, time))");

        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:14:00 UTC', 3)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:15:00 UTC', 4)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:21:00 UTC', 5)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:22:00 UTC', 6)");

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND time > ? - 5m", toTimestamp("2016-09-27 16:20:00 UTC")),
                   row(1, toTimestamp("2016-09-27 16:21:00 UTC"), 5),
                   row(1, toTimestamp("2016-09-27 16:22:00 UTC"), 6));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND time >= ? - 10m", toTimestamp("2016-09-27 16:25:00 UTC")),
                   row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4),
                   row(1, toTimestamp("2016-09-27 16:21:00 UTC"), 5),
                   row(1, toTimestamp("2016-09-27 16:22:00 UTC"), 6));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND time >= ? + 5m", toTimestamp("2016-09-27 16:15:00 UTC")),
                   row(1, toTimestamp("2016-09-27 16:21:00 UTC"), 5),
                   row(1, toTimestamp("2016-09-27 16:22:00 UTC"), 6));

        assertRows(execute("SELECT time - 10m FROM %s WHERE pk = 1"),
                   row(toTimestamp("2016-09-27 16:00:00 UTC")),
                   row(toTimestamp("2016-09-27 16:02:00 UTC")),
                   row(toTimestamp("2016-09-27 16:04:00 UTC")),
                   row(toTimestamp("2016-09-27 16:05:00 UTC")),
                   row(toTimestamp("2016-09-27 16:11:00 UTC")),
                   row(toTimestamp("2016-09-27 16:12:00 UTC")));

        assertInvalidMessage("the '%' operation is not supported between time and 10m",
                             "SELECT time %% 10m FROM %s WHERE pk = 1");
        assertInvalidMessage("the '*' operation is not supported between time and 10m",
                             "SELECT time * 10m FROM %s WHERE pk = 1");
        assertInvalidMessage("the '/' operation is not supported between time and 10m",
                             "SELECT time / 10m FROM %s WHERE pk = 1");
        assertInvalidMessage("the operation 'timestamp - duration' failed: The duration must have a millisecond precision. Was: 10us",
                             "SELECT * FROM %s WHERE pk = 1 AND time > ? - 10us", toTimestamp("2016-09-27 16:15:00 UTC"));

        // Test with date type.
        createTable("CREATE TABLE %s (pk int, time date, v int, primary key (pk, time))");

        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27', 1)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-28', 2)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-29', 3)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-30', 4)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-01', 5)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-04', 6)");

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND time > ? - 5d", toDate("2016-10-04")),
                   row(1, toDate("2016-09-30"), 4),
                   row(1, toDate("2016-10-01"), 5),
                   row(1, toDate("2016-10-04"), 6));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND time > ? - 6d", toDate("2016-10-04")),
                   row(1, toDate("2016-09-29"), 3),
                   row(1, toDate("2016-09-30"), 4),
                   row(1, toDate("2016-10-01"), 5),
                   row(1, toDate("2016-10-04"), 6));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND time >= ? + 1d",  toDate("2016-10-01")),
                   row(1, toDate("2016-10-04"), 6));

        assertRows(execute("SELECT time - 2d FROM %s WHERE pk = 1"),
                   row(toDate("2016-09-25")),
                   row(toDate("2016-09-26")),
                   row(toDate("2016-09-27")),
                   row(toDate("2016-09-28")),
                   row(toDate("2016-09-29")),
                   row(toDate("2016-10-02")));

        assertInvalidMessage("the '%' operation is not supported between time and 10m",
                             "SELECT time %% 10m FROM %s WHERE pk = 1");
        assertInvalidMessage("the '*' operation is not supported between time and 10m",
                             "SELECT time * 10m FROM %s WHERE pk = 1");
        assertInvalidMessage("the '/' operation is not supported between time and 10m",
                             "SELECT time / 10m FROM %s WHERE pk = 1");
        assertInvalidMessage("the operation 'date - duration' failed: The duration must have a day precision. Was: 10m",
                             "SELECT * FROM %s WHERE pk = 1 AND time > ? - 10m", toDate("2016-10-04"));
    }

    private Date toTimestamp(String timestampAsString)
    {
        return new Date(TimestampSerializer.dateStringToTimestamp(timestampAsString));
    }

    private int toDate(String dateAsString)
    {
        return SimpleDateSerializer.dateStringToDays(dateAsString);
    }

    @Test
    public void testFunctionException() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 int, c2 int, v text, PRIMARY KEY(pk, c1, c2))");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 0, 2, 'test')");

        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "the operation 'int / int' failed: / by zero",
                                  com.datastax.driver.core.exceptions.FunctionExecutionException.class,
                                  "SELECT c2 / c1 FROM %s WHERE pk = 1");
    }

}

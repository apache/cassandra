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
package org.apache.cassandra.index.sai.cql;

import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.types.InetTest;

import static org.apache.cassandra.index.sai.utils.IPv6v4ComparisonSupport.NOT_IP_ERROR;

/**
 * This is testing that we can query ipv4 addresses using ipv6 equivalent addresses
 * in case 'compare_v4_to_v6_as_equal': 'true'.
 * <p>
 * The remaining InetAddressType tests are now handled by {@link InetTest}
 */
public class InetAddressTypeEquivalencyTest extends SAITester
{
    @Before
    public void createTable()
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk int, ck int, ip inet, val text, PRIMARY KEY(pk, ck ))");

        disableCompaction();
    }

    @Test
    public void testInetRangeQuery()
    {
        createTable("CREATE TABLE %s (pk int, val inet, PRIMARY KEY(pk))");

        // Addresses are added in ascending order according to the InetAddressType.
        execute("INSERT INTO %s (pk, val) VALUES (0, '0.51.33.51')");
        execute("INSERT INTO %s (pk, val) VALUES (1, '267:41f9:3b96:7ea5:c825:a0aa:aac8:5164')");
        execute("INSERT INTO %s (pk, val) VALUES (2, '3.199.227.48')");
        execute("INSERT INTO %s (pk, val) VALUES (3, '6.7.108.133')");
        execute("INSERT INTO %s (pk, val) VALUES (4, '7f5:1c0b:238:987d:18dd:e06b:ba16:a36')");

        // Confirm result when there isn't an index
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE val > '3.199.227.48' ALLOW FILTERING"),
                row(3), row(4));

        String index = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(index);

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE val > '3.199.227.48'"),
                row(3), row(4));
    }

    @Test
    public void testNonIpIndexWithCompareOptionTrue() throws Throwable
    {
        assertIndexThrowsNotIPError( "{ 'compare_v4_to_v6_as_equal': 'true' }");
    }

    @Test
    public void testNonIpIndexWithCompareOptionFalse() throws Throwable
    {
        assertIndexThrowsNotIPError( "{ 'compare_v4_to_v6_as_equal': 'false' }");
    }

    @Test
    public void testNonIpIndexWithCompareOptionWithWrongValue() throws Throwable
    {
        assertIndexThrowsNotIPError( "{ 'compare_v4_to_v6_as_equal': 'WRONG' }");
    }

    private void assertIndexThrowsNotIPError(String indexOptions) throws Throwable {
        assertInvalidMessage(NOT_IP_ERROR,
                "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS =" + indexOptions);
    }

    @Test
    public void testIpQueriesFiltering() throws Throwable
    {
        populateTable();
        runQueries(false, true);
    }

    @Test
    public void testIpIndexWithDefaults() throws Throwable
    {
        createIndex("CREATE CUSTOM INDEX ON %s(ip) USING 'StorageAttachedIndex'");
        populateTable();
        mixedWorkloadQuery(false, false);
    }

    @Test
    public void testIpIndexWithCompareEqualTrue() throws Throwable
    {
        createIndex("CREATE CUSTOM INDEX ON %s(ip) USING 'StorageAttachedIndex' WITH OPTIONS = { 'compare_v4_to_v6_as_equal': 'true' }");
        populateTable();
        mixedWorkloadQuery(true, false);
    }

    @Test
    public void testIpIndexWithCompareEqualFalse() throws Throwable
    {
        createIndex("CREATE CUSTOM INDEX ON %s(ip) USING 'StorageAttachedIndex' WITH OPTIONS = { 'compare_v4_to_v6_as_equal': 'false' }");
        populateTable();
        mixedWorkloadQuery(false, false);
    }

    public void mixedWorkloadQuery(boolean eq, boolean allowFiltering) throws Throwable
    {
        runQueries(eq, allowFiltering);
    }

    private void populateTable ()
    {
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");

        flush();

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");

        flush();

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

    }

    private void runQueries(boolean eq, boolean allowFiltering) throws Throwable
    {
        String msg = "";
        if (allowFiltering)
            msg = "ALLOW FILTERING";

        // EQ single ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip = '127.0.0.1'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null)
                });

        // EQ mapped-ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip = '::ffff:7f00:1'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null)
                });

        // EQ ipv6 address
        compareQueryResults("SELECT * FROM %s WHERE ip = '2002:4559:1fe2::4559:1fe2'" + msg, eq,
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null)
                },
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null)
                });

        // GT ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip > '127.0.0.1'" + msg, eq,
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null)
                });

        // GT mapped-ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip > '::ffff:7f00:1'" + msg, eq,
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null)
                });

        // GT ipv6 address
        compareQueryResults("SELECT * FROM %s WHERE ip > '2002:4559:1fe2::4559:1fe2'" + msg, eq,
                new Object[][] {
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                });

        // LT ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip < '127.0.0.3'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                });

        // LT mapped-ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip < '::ffff:7f00:3'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                });

        // LT ipv6 address
        compareQueryResults("SELECT * FROM %s WHERE ip < '2002:4559:1fe2::4559:1fe3'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null)
                },
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null)
                });

        // GE ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip >= '127.0.0.2'" + msg, eq,
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null)
                });

        // GE mapped-ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip >= '::ffff:7f00:2'" + msg, eq,
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null)
                });

        // GE ipv6 address
        compareQueryResults("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe3'" + msg, eq,
                new Object[][] {
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                });

        // LE ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip <= '127.0.0.2'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                });

        // LE mapped-ipv4 address
        compareQueryResults("SELECT * FROM %s WHERE ip <= '::ffff:7f00:2'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null)
                });

        // LE ipv6 address
        compareQueryResults("SELECT * FROM %s WHERE ip <= '2002:4559:1fe2::4559:1fe2'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null)
                },
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null)
                });

        // ipv4 range
        compareQueryResults("SELECT * FROM %s WHERE ip >= '127.0.0.1' AND ip <= '127.0.0.3'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("127.0.0.3"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("127.0.0.3"), null)
                });

        // ipv4 and mapped ipv4 range
        compareQueryResults("SELECT * FROM %s WHERE ip >= '127.0.0.1' AND ip <= '::ffff:7f00:3'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("127.0.0.3"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("127.0.0.3"), null)
                });

        // ipv6 range
        compareQueryResults("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe2' AND ip <= '2002:4559:1fe2::4559:1fe3'" + msg, eq,
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                });

        // ipv6 range
        compareQueryResults("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe2' AND ip <= '2002:4559:1fe2::4559:1fe3'" + msg, eq,
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                });

        // Full ipv6 range
        compareQueryResults("SELECT * FROM %s WHERE ip >= '::' AND ip <= 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'" + msg, eq,
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                },
                new Object[][] {
                        row(1, 1, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 2, InetAddress.getByName("127.0.0.1"), null),
                        row(1, 3, InetAddress.getByName("127.0.0.2"), null),
                        row(1, 4, InetAddress.getByName("::ffff:7f00:3"), null),
                        row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2"), null),
                        row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3"), null)
                });
    }

    public void compareQueryResults(String query, boolean eq, Object[][] rowsTrue, Object[][] rowsFalse)
    {
        if (eq)
            assertRowsIgnoringOrder(execute(query), rowsTrue);
        else
            assertRowsIgnoringOrder(execute(query), rowsFalse);
    }
}

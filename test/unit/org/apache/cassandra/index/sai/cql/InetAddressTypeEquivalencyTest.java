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

/**
 * This is testing that we can query ipv4 addresses using ipv6 equivalent addresses.
 *
 * The remaining InetAddressType tests are now handled by {@link InetTest}
 */
public class InetAddressTypeEquivalencyTest extends SAITester
{
    @Before
    public void createTableAndIndex()
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk int, ck int, ip inet, PRIMARY KEY(pk, ck ))");

        disableCompaction();
    }

    @Test
    public void mixedWorkloadQueryTest() throws Throwable
    {
        createIndex("CREATE CUSTOM INDEX ON %s(ip) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

        runQueries();
    }

    private void runQueries() throws Throwable
    {
        // EQ single ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip = '127.0.0.1'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")));

        // EQ mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip = '::ffff:7f00:1'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")));

        // EQ ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip = '2002:4559:1fe2::4559:1fe2'"),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")));

        // GT ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip > '127.0.0.1'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GT mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip > '::ffff:7f00:1'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GT ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip > '2002:4559:1fe2::4559:1fe2'"),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // LT ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip < '127.0.0.3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LT mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip < '::ffff:7f00:3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LT ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip < '2002:4559:1fe2::4559:1fe3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")));

        // GE ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip >= '127.0.0.2'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GE mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip >= '::ffff:7f00:2'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GE ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe3'"),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // LE ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip <= '127.0.0.2'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LE mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip <= '::ffff:7f00:2'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LE ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip <= '2002:4559:1fe2::4559:1fe2'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")));

        // ipv4 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '127.0.0.1' AND ip <= '127.0.0.3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("127.0.0.3")));

        // ipv4 and mapped ipv4 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '127.0.0.1' AND ip <= '::ffff:7f00:3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("127.0.0.3")));

        // ipv6 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe2' AND ip <= '2002:4559:1fe2::4559:1fe3'"),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // Full ipv6 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '::' AND ip <= 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));
    }
}

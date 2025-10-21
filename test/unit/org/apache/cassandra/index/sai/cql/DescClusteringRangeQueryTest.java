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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.index.sai.SAITester;

public class DescClusteringRangeQueryTest extends SAITester
{
    @Test
    public void testReversedIntBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c int, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        beforeAndAfterFlush(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, 3, "MA"), row (0, 2, "MA"));
            ResultSet betweenRowsNet = executeNet("SELECT * FROM %s WHERE c BETWEEN 2 AND 3 AND abbreviation = 'MA'");
            assertRowsNet(betweenRowsNet, row (0, 3, "MA"), row (0, 2, "MA"));
        });
    }

    @Test
    public void testReversedLongBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c bigint, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        beforeAndAfterFlush(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, 3L, "MA"), row (0, 2L, "MA"));
            ResultSet betweenRowsNet = executeNet("SELECT * FROM %s WHERE c BETWEEN 2 AND 3 AND abbreviation = 'MA'");
            assertRowsNet(betweenRowsNet, row (0, 3L, "MA"), row (0, 2L, "MA"));
        });
    }

    @Test
    public void testReversedBigIntegerBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c varint, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        beforeAndAfterFlush(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, new BigInteger("3"), "MA"), row (0, new BigInteger("2"), "MA"));
            ResultSet betweenRowsNet = executeNet("SELECT * FROM %s WHERE c BETWEEN 2 AND 3 AND abbreviation = 'MA'");
            assertRowsNet(betweenRowsNet, row (0, new BigInteger("3"), "MA"), row (0, new BigInteger("2"), "MA"));
        });
    }

    @Test
    public void testReversedBigDecimalBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c decimal, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1.1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2.1, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2.9, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4.0, 'TX')");

        beforeAndAfterFlush(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c > 1.9 AND c < 3.0 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, new BigDecimal("2.9"), "MA"), row (0, new BigDecimal("2.1"), "MA"));
            ResultSet betweenRowsNet = executeNet("SELECT * FROM %s WHERE c BETWEEN 1.9 AND 3.0 AND abbreviation = 'MA'");
            assertRowsNet(betweenRowsNet, row (0, new BigDecimal("2.9"), "MA"), row (0, new BigDecimal("2.1"), "MA"));
        });
    }

    @Test
    public void testReversedInetBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c inet, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.1', 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.2', 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.3', 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.4', 'TX')");

        beforeAndAfterFlush(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= '127.0.0.2' AND c <= '127.0.0.3' AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, InetAddress.getByName("127.0.0.3"), "MA"), row (0, InetAddress.getByName("127.0.0.2"), "MA"));
            ResultSet betweenRowsNet = executeNet("SELECT * FROM %s WHERE c BETWEEN '127.0.0.2' AND '127.0.0.3' AND abbreviation = 'MA'");
            assertRowsNet(betweenRowsNet, row (0, InetAddress.getByName("127.0.0.3"), "MA"), row (0, InetAddress.getByName("127.0.0.2"), "MA"));
        });
    }

    @Test
    public void testReversedIntBetweenWithAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c int, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        beforeAndAfterFlush(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, 3, "MA"), row (0, 2, "MA"));
            ResultSet betweenRowsNet = executeNet("SELECT * FROM %s WHERE c BETWEEN 2 AND 3 AND abbreviation = 'MA'");
            assertRowsNet(betweenRowsNet, row (0, 3, "MA"), row (0, 2, "MA"));
        });
    }
}

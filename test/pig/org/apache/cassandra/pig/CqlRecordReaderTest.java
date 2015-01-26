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
package org.apache.cassandra.pig;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;

public class CqlRecordReaderTest extends PigTestBase
{
    private static String[] statements = {
        "DROP KEYSPACE IF EXISTS cql3ks",
        "CREATE KEYSPACE cql3ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};",
        "USE cql3ks;",

        "CREATE TABLE cqltable (" +
        "pk1 int," +
        "pk2 int," +
        "pk3 int," +
        "ck1 int," +
        "ck2 int," +
        "data text," +
        "primary key((pk1,pk2,pk3),ck1,ck2));",
        "INSERT INTO cqltable(pk1, pk2, pk3, ck1, ck2, data) VALUES (11, 12, 13, 14, 15, 'value1');",

        "CREATE TABLE \"MixedCaseCqlTable\" (" +
        "pk1 int," +
        "\"PK2\" int," +
        "pk3 int," +
        "\"CK1\" int," +
        "ck2 int," +
        "data text," +
        "primary key((pk1,\"PK2\",pk3),\"CK1\",ck2));",
        "INSERT INTO \"MixedCaseCqlTable\"(pk1, \"PK2\", pk3, \"CK1\", ck2, data) VALUES (11, 12, 13, 14, 15, 'value1');",
    };

    @BeforeClass
    public static void setup() throws IOException, ConfigurationException, TException
    {
        startCassandra();
        executeCQLStatements(statements);
        startHadoopCluster();
    }

    @Test
    public void defaultCqlQueryTest() throws Exception
    {
        String initialQuery = "rows = LOAD 'cql://cql3ks/cqltable?" + defaultParameters + nativeParameters + "' USING CqlNativeStorage();";
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 11);
            Assert.assertEquals(t.get(1), 12);
            Assert.assertEquals(t.get(2), 13);
            Assert.assertEquals(t.get(3), 14);
            Assert.assertEquals(t.get(4), 15);
            Assert.assertEquals(t.get(5), "value1");
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void defaultMixedCaseCqlQueryTest() throws Exception
    {
        String initialQuery = "rows = LOAD 'cql://cql3ks/MixedCaseCqlTable?" + defaultParameters + nativeParameters + "' USING CqlNativeStorage();";
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 11);
            Assert.assertEquals(t.get(1), 12);
            Assert.assertEquals(t.get(2), 13);
            Assert.assertEquals(t.get(3), 14);
            Assert.assertEquals(t.get(4), 15);
            Assert.assertEquals(t.get(5), "value1");
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void selectColumnsTest() throws Exception
    {
        String initialQuery = "rows = LOAD 'cql://cql3ks/cqltable?" + defaultParameters + nativeParameters + "&columns=ck1%2Cck2%2Cdata' USING CqlNativeStorage();";
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 11);
            Assert.assertEquals(t.get(1), 12);
            Assert.assertEquals(t.get(2), 13);
            Assert.assertEquals(t.get(3), 14);
            Assert.assertEquals(t.get(4), 15);
            Assert.assertEquals(t.get(5), "value1");
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void whereClauseTest() throws Exception
    {
        String initialQuery = "rows = LOAD 'cql://cql3ks/cqltable?" + defaultParameters + nativeParameters + "&where_clause=ck1%3d14' USING CqlNativeStorage();";
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 11);
            Assert.assertEquals(t.get(1), 12);
            Assert.assertEquals(t.get(2), 13);
            Assert.assertEquals(t.get(3), 14);
            Assert.assertEquals(t.get(4), 15);
            Assert.assertEquals(t.get(5), "value1");
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }
}

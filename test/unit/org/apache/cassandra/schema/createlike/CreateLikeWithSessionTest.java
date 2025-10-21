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

package org.apache.cassandra.schema.createlike;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CreateLikeWithSessionTest extends CQLTester
{
    private static String ks1 = "ks1";
    private static String ks2 = "ks2";
    private static String tb1 = "tb1";
    private static String tb2 = "tb2";

    @BeforeClass
    public static void beforeClass()
    {
        requireNetwork();
    }

    @Test
    public void testCreateLikeWithSession()
    {
        // create two keyspaces and tables
        createKeyspace("CREATE KEYSPACE " + ks1 + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createKeyspace("CREATE KEYSPACE " + ks2 + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable("CREATE TABLE " + ks1 + "." + tb1 + " (id int PRIMARY KEY, age int);");
        createTable("CREATE TABLE " + ks2 + "." + tb2 + " (name text PRIMARY KEY, address text);");

        // use ks1
        executeNet("use " + ks1);
        executeNet("CREATE TABLE tb3 LIKE " + tb1);
        executeNet("CREATE TABLE " + ks1 + ".tb4 LIKE " + tb1);
        executeNet("CREATE TABLE tb5 like " + ks1 + "." + tb1);

        executeNet("CREATE TABLE " + ks2 + ".tb6 LIKE " + tb1);


        assertThatExceptionOfType(com.datastax.driver.core.exceptions.InvalidQueryException.class).isThrownBy(() -> executeNet("CREATE TABLE tb7 LIKE " + ks2 + "." + tb1))
                                                                                                  .withMessage("Souce Table 'ks2.tb1' doesn't exist");

        assertNotNull(getTableMetadata(ks1, tb1));
        assertNotNull(getTableMetadata(ks1, "tb3"));
        assertNotNull(getTableMetadata(ks1, "tb4"));
        assertNotNull(getTableMetadata(ks1, "tb5"));
        assertNotNull(getTableMetadata(ks2, "tb6"));
        assertNull(getTableMetadata(ks2, "tb7"));
    }
}

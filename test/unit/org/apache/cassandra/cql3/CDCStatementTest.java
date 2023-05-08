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

package org.apache.cassandra.cql3;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;

public class CDCStatementTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setCDCEnabled(true);
        CQLTester.setUpClass();
    }

    @Test
    public void testEnableOnCreate() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key)) WITH cdc = true;");
        Assert.assertTrue(currentTableMetadata().params.cdc);
    }

    @Test
    public void testEnableOnAlter() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key));");
        Assert.assertFalse(currentTableMetadata().params.cdc);
        execute("ALTER TABLE %s WITH cdc = true;");
        Assert.assertTrue(currentTableMetadata().params.cdc);
    }

    @Test
    public void testDisableOnAlter() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key)) WITH cdc = true;");
        Assert.assertTrue(currentTableMetadata().params.cdc);
        execute("ALTER TABLE %s WITH cdc = false;");
        Assert.assertFalse(currentTableMetadata().params.cdc);
    }
}

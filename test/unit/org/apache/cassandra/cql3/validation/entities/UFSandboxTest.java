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

package org.apache.cassandra.cql3.validation.entities;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;
import static org.junit.Assert.assertNull;

/** Runs the security and timeout tests without an installed security manager. */
public class UFSandboxTest extends UFSecurityTest
{
    @BeforeClass
    public static void setUpClass()
    {
        UDF_SECURITY_MECHANISM.setString("sandbox");
        CQLTester.setUpClass();
    }

    @Test
    public void noSecurityManager()
    {
        assertNull(System.getSecurityManager());
    }

    @Test
    public void fileFormatterIsRejected() throws Throwable
    {
        assertInvalid("CREATE FUNCTION " + KEYSPACE + ".invalid_formatter(val double) " +
                      "RETURNS NULL ON NULL INPUT RETURNS double LANGUAGE JAVA " +
                      "AS 'try { new java.util.Formatter(\"udf-sandbox-test\"); } catch (Exception e) {} return 0d;'");
    }
}

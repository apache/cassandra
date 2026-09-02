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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.security.ThreadAwareSecurityManager;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;

/**
 * Tests {@code allow_extra_insecure_udfs} with the byte-code sandbox. The verifier permits System calls when
 * UDF threads are disabled and this option is true. A separate class prevents this configuration from
 * affecting other tests.
 */
public class UFInsecureSystemAccessTest extends CQLTester
{
    @BeforeClass
    public static void setUpInsecureUDFs()
    {
        // Select the byte-code sandbox before creating a function.
        UDF_SECURITY_MECHANISM.setString("sandbox");

        // This configuration permits System calls in UDFs.
        Config conf = DatabaseDescriptor.getRawConfig();
        conf.user_defined_functions_enabled = true;
        conf.user_defined_functions_threads_enabled = false;
        conf.allow_extra_insecure_udfs = true;
    }

    /** Confirms that the configured exception permits System calls and property aliases. */
    @Test
    public void testExtraInsecureUDFsAllowSystemAccess() throws Throwable
    {
        Assume.assumeFalse("legacy SecurityManager mechanism in use; the insecure combo is enforced by the SM, not the verifier",
                           ThreadAwareSecurityManager.useSecurityManager());

        // Confirm the test configuration.
        Assert.assertFalse(DatabaseDescriptor.enableUserDefinedFunctionsThreads());
        Assert.assertTrue(DatabaseDescriptor.allowExtraInsecureUDFs());

        String[][] sources =
        {
        {"getProperty",     "System.getProperty(\"foo\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getenv",          "System.getenv(\"PATH\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getlogger",       "System.getLogger(\"x\"); return 0d;"},
        {"getinteger",      "Integer.getInteger(\"x\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getlong",         "Long.getLong(\"x\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getboolean",      "Boolean.getBoolean(\"x\"); return 0d;"} // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        };

        for (String[] s : sources)
        {
            String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS double " +
                                          "LANGUAGE JAVA\n" +
                                          "AS '" + s[1] + "';");
            Assert.assertNotNull("expected insecure UDF using System." + s[0] + " to be created", fName);
        }
    }
}

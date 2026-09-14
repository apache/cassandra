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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.security.ThreadAwareSecurityManager;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;
import static org.junit.Assert.assertEquals;

/** Tests both settings required for insecure system access. */
public class UFInsecureSystemAccessTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        UDF_SECURITY_MECHANISM.setString("sandbox");
        CQLTester.setUpClass();
    }

    @Test
    public void systemAccessRequiresBothSettings() throws Throwable
    {
        assertEquals(ThreadAwareSecurityManager.useSecurityManager(), System.getSecurityManager() != null);
        createTable("CREATE TABLE %s (key int PRIMARY KEY, val double)");
        execute("INSERT INTO %s (key, val) VALUES (1, 0)");
        Config conf = DatabaseDescriptor.getRawConfig();
        boolean threads = conf.user_defined_functions_threads_enabled;
        boolean insecure = conf.allow_extra_insecure_udfs;
        String[] sources = {
            "System.getProperty(\"java.version\"); return 0d;", // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            "System.getenv(\"PATH\"); return 0d;", // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            "Integer.getInteger(\"udf-test\"); return 0d;", // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            "Long.getLong(\"udf-test\"); return 0d;", // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            "Boolean.getBoolean(\"udf-test\"); return 0d;" // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        };
        try
        {
            for (boolean useThreads : new boolean[]{ false, true })
            {
                for (boolean allowInsecure : new boolean[]{ false, true })
                {
                    conf.user_defined_functions_threads_enabled = useThreads;
                    conf.allow_extra_insecure_udfs = allowInsecure;
                    for (String source : sources)
                    {
                        if (!useThreads && allowInsecure)
                        {
                            String name = createFunction(KEYSPACE_PER_TEST, "double", function("%s", source));
                            assertRows(execute("SELECT " + name + "(val) FROM %s WHERE key=1"), row(0d));
                        }
                        else if (useThreads && ThreadAwareSecurityManager.useSecurityManager())
                        {
                            String name = createFunction(KEYSPACE_PER_TEST, "double", function("%s", source));
                            assertInvalidMessage("access denied", "SELECT " + name + "(val) FROM %s WHERE key=1");
                        }
                        else
                        {
                            assertInvalid(function(KEYSPACE + ".restricted", source));
                        }
                    }
                    // System.getLogger is on the sandbox verifier's System deny list, so under the sandbox mechanism
                    // only the insecure combination may create a function that calls it.
                    if (!useThreads && allowInsecure)
                    {
                        String name = createFunction(KEYSPACE_PER_TEST, "double",
                                                     function("%s", "System.getLogger(\"udf-test\"); return 0d;"));
                        assertRows(execute("SELECT " + name + "(val) FROM %s WHERE key=1"), row(0d));
                    }
                    assertInvalidMessage("call to java.lang.ClassLoader.getPlatformClassLoader()",
                                         function(KEYSPACE + ".restricted", "ClassLoader.getPlatformClassLoader(); return 0d;"));
                    assertInvalid(function(KEYSPACE + ".restricted", "Runtime.getRuntime(); return 0d;"));
                }
            }
        }
        finally
        {
            conf.user_defined_functions_threads_enabled = threads;
            conf.allow_extra_insecure_udfs = insecure;
        }
    }

    private static String function(String name, String source)
    {
        return "CREATE OR REPLACE FUNCTION " + name + "(val double) RETURNS NULL ON NULL INPUT " +
               "RETURNS double LANGUAGE JAVA AS '" + source + "';";
    }
}

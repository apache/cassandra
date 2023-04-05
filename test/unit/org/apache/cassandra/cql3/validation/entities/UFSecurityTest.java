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

import java.security.AccessControlException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.service.ClientWarn;

public class UFSecurityTest extends CQLTester
{
    @Test
    public void testSecurityPermissions() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        // Java UDFs

        try
        {
            String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS double " +
                                          "LANGUAGE JAVA\n" +
                                          "AS 'System.getProperty(\"foo.bar.baz\"); return 0d;';");
            execute("SELECT " + fName + "(dval) FROM %s WHERE key=1");
            Assert.fail();
        }
        catch (FunctionExecutionException e)
        {
            assertAccessControlException("System.getProperty(\"foo.bar.baz\"); return 0d;", e);
        }

        String[] cfnSources =
        { "try { Class.forName(\"" + UDHelper.class.getName() + "\"); } catch (Exception e) { throw new RuntimeException(e); } return 0d;",
          "try { Class.forName(\"sun.misc.Unsafe\"); } catch (Exception e) { throw new RuntimeException(e); } return 0d;" };
        for (String source : cfnSources)
        {
            assertInvalidMessage("Java UDF validation failed: [call to java.lang.Class.forName()]",
                                 "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_class_access(val double) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS double " +
                                 "LANGUAGE JAVA\n" +
                                 "AS '" + source + "';");
        }

        String[][] typesAndSources =
        {
        {"sun.misc.Unsafe",         "sun.misc.Unsafe.getUnsafe(); return 0d;"},
        {"java.nio.file.FileSystems", "try {" +
                                      "     java.nio.file.FileSystems.getDefault(); return 0d;" +
                                      "} catch (Exception t) {" +
                                      "     throw new RuntimeException(t);" +
                                      '}'},
        {"java.nio.channels.FileChannel", "try {" +
                                          "     java.nio.channels.FileChannel.open(java.nio.file.FileSystems.getDefault().getPath(\"/etc/passwd\")).close(); return 0d;" +
                                          "} catch (Exception t) {" +
                                          "     throw new RuntimeException(t);" +
                                          '}'},
        {"java.nio.channels.SocketChannel", "try {" +
                                            "     java.nio.channels.SocketChannel.open().close(); return 0d;" +
                                            "} catch (Exception t) {" +
                                            "     throw new RuntimeException(t);" +
                                            '}'},
        {"java.io.FileInputStream", "try {" +
                                    "     new java.io.FileInputStream(\"./foobar\").close(); return 0d;" +
                                    "} catch (Exception t) {" +
                                    "     throw new RuntimeException(t);" +
                                    '}'},
        {"java.lang.Runtime",       "try {" +
                                    "     java.lang.Runtime.getRuntime(); return 0d;" +
                                    "} catch (Exception t) {" +
                                    "     throw new RuntimeException(t);" +
                                    '}'},
        {"org.apache.cassandra.service.StorageService",
         "try {" +
         "     org.apache.cassandra.service.StorageService v = org.apache.cassandra.service.StorageService.instance; v.isShutdown(); return 0d;" +
         "} catch (Exception t) {" +
         "     throw new RuntimeException(t);" +
         '}'},
        {"java.net.ServerSocket",   "try {" +
                                    "     new java.net.ServerSocket().bind(); return 0d;" +
                                    "} catch (Exception t) {" +
                                    "     throw new RuntimeException(t);" +
                                    '}'},
        {"java.io.FileOutputStream","try {" +
                                    "     new java.io.FileOutputStream(\".foo\"); return 0d;" +
                                    "} catch (Exception t) {" +
                                    "     throw new RuntimeException(t);" +
                                    '}'},
        {"java.lang.Runtime",       "try {" +
                                    "     java.lang.Runtime.getRuntime().exec(\"/tmp/foo\"); return 0d;" +
                                    "} catch (Exception t) {" +
                                    "     throw new RuntimeException(t);" +
                                    '}'}
        };

        for (String[] typeAndSource : typesAndSources)
        {
            assertInvalidMessage(typeAndSource[0] + " cannot be resolved",
                                 "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_class_access(val double) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS double " +
                                 "LANGUAGE JAVA\n" +
                                 "AS '" + typeAndSource[1] + "';");
        }

        // JavaScript UDFs

        try
        {
            String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS double " +
                                          "LANGUAGE javascript\n" +
                                          "AS 'org.apache.cassandra.service.StorageService.instance.isShutdown(); 0;';");
            execute("SELECT " + fName + "(dval) FROM %s WHERE key=1");
            Assert.fail("Javascript security check failed");
        }
        catch (FunctionExecutionException e)
        {
            assertAccessControlException("", e);
        }

        String[] javascript =
        {
        "java.lang.management.ManagmentFactory.getThreadMXBean(); 0;",
        "new java.io.FileInputStream(\"/tmp/foo\"); 0;",
        "new java.io.FileOutputStream(\"/tmp/foo\"); 0;",
        "java.nio.file.FileSystems.getDefault().createFileExclusively(\"./foo_bar_baz\"); 0;",
        "java.nio.channels.FileChannel.open(java.nio.file.FileSystems.getDefault().getPath(\"/etc/passwd\")); 0;",
        "java.nio.channels.SocketChannel.open(); 0;",
        "new java.net.ServerSocket().bind(null); 0;",
        "var thread = new java.lang.Thread(); thread.start(); 0;",
        "java.lang.System.getProperty(\"foo.bar.baz\"); 0;",
        "java.lang.Runtime.getRuntime().exec(\"/tmp/foo\"); 0;",
        "java.lang.Runtime.getRuntime().loadLibrary(\"foobar\"); 0;",
        "java.lang.Runtime.getRuntime().loadLibrary(\"foobar\"); 0;",
        // TODO these (ugly) calls are still possible - these can consume CPU (as one could do with an evil loop, too)
//        "java.lang.Runtime.getRuntime().traceMethodCalls(true); 0;",
//        "java.lang.Runtime.getRuntime().gc(); 0;",
//        "java.lang.Runtime.getRuntime(); 0;",
        };

        for (String script : javascript)
        {
            try
            {
                String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                              "CREATE OR REPLACE FUNCTION %s(val double) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS double " +
                                              "LANGUAGE javascript\n" +
                                              "AS '" + script + "';");
                execute("SELECT " + fName + "(dval) FROM %s WHERE key=1");
                Assert.fail("Javascript security check failed: " + script);
            }
            catch (FunctionExecutionException e)
            {
                assertAccessControlException(script, e);
            }
        }

        String script = "java.lang.Class.forName(\"java.lang.System\"); 0;";
        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE javascript\n" +
                                      "AS '" + script + "';");
        assertInvalidThrowMessage("Java reflection not supported when class filter is present",
                                  FunctionExecutionException.class,
                                  "SELECT " + fName + "(dval) FROM %s WHERE key=1");
    }

    private static void assertAccessControlException(String script, FunctionExecutionException e)
    {
        for (Throwable t = e; t != null && t != t.getCause(); t = t.getCause())
            if (t instanceof AccessControlException)
                return;
        Assert.fail("no AccessControlException for " + script + " (got " + e + ')');
    }

    @Test
    public void testAmokUDF() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        long udfWarnTimeout = DatabaseDescriptor.getUserDefinedFunctionWarnTimeout();
        long udfFailTimeout = DatabaseDescriptor.getUserDefinedFunctionFailTimeout();
        int maxTries = 5;
        for (int i = 1; i <= maxTries; i++)
        {
            try
            {
                // short timeout
                DatabaseDescriptor.setUserDefinedFunctionWarnTimeout(10);
                DatabaseDescriptor.setUserDefinedFunctionFailTimeout(250);
                // don't kill the unit test... - default policy is "die"
                DatabaseDescriptor.setUserFunctionTimeoutPolicy(Config.UserFunctionTimeoutPolicy.ignore);

                ClientWarn.instance.captureWarnings();
                String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                              "CREATE OR REPLACE FUNCTION %s(val double) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS double " +
                                              "LANGUAGE JAVA\n" +
                                              "AS 'long t=System.currentTimeMillis()+110; while (t>System.currentTimeMillis()) { }; return 0d;'");
                execute("SELECT " + fName + "(dval) FROM %s WHERE key=1");
                List<String> warnings = ClientWarn.instance.getWarnings();
                Assert.assertNotNull(warnings);
                Assert.assertFalse(warnings.isEmpty());
                ClientWarn.instance.resetWarnings();

                // Java UDF

                fName = createFunction(KEYSPACE_PER_TEST, "double",
                                       "CREATE OR REPLACE FUNCTION %s(val double) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS double " +
                                       "LANGUAGE JAVA\n" +
                                       "AS 'long t=System.currentTimeMillis()+500; while (t>System.currentTimeMillis()) { }; return 0d;';");
                assertInvalidMessage("ran longer than 250ms", "SELECT " + fName + "(dval) FROM %s WHERE key=1");

                // Javascript UDF

                fName = createFunction(KEYSPACE_PER_TEST, "double",
                                       "CREATE OR REPLACE FUNCTION %s(val double) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS double " +
                                       "LANGUAGE JAVASCRIPT\n" +
                                       "AS 'var t=java.lang.System.currentTimeMillis()+500; while (t>java.lang.System.currentTimeMillis()) { }; 0;';");
                assertInvalidMessage("ran longer than 250ms", "SELECT " + fName + "(dval) FROM %s WHERE key=1");

                return;
            }
            catch (Error | RuntimeException e)
            {
                if (i == maxTries)
                    throw e;
            }
            finally
            {
                // reset to defaults
                DatabaseDescriptor.setUserDefinedFunctionWarnTimeout(udfWarnTimeout);
                DatabaseDescriptor.setUserDefinedFunctionFailTimeout(udfFailTimeout);
            }
        }
    }

}

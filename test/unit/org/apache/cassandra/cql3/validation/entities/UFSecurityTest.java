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
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.security.ThreadAwareSecurityManager;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.JavaDriverUtils;

public class UFSecurityTest extends CQLTester
{
    @Test
    public void testSecurityPermissions() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        // Java UDFs

        // UDFs must load System for timing and array operations. A SecurityManager blocks getProperty at runtime.
        // The byte-code sandbox blocks it when the function is created.
        if (ThreadAwareSecurityManager.useSecurityManager())
        {
            try
            {
                String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                              "CREATE OR REPLACE FUNCTION %s(val double) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS double " +
                                              "LANGUAGE JAVA\n" +
                                              "AS 'System.getProperty(\"foo.bar.baz\"); return 0d;';"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
                execute("SELECT " + fName + "(dval) FROM %s WHERE key=1");
                Assert.fail();
            }
            catch (FunctionExecutionException e)
            {
                assertAccessControlException("System.getProperty(\"foo.bar.baz\"); return 0d;", e); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            }
        }
        else
        {
            assertInvalidMessage("Java UDF validation failed: [call to java.lang.System.getProperty()]", // checkstyle: suppress nearby 'blockSystemPropertyUsage'
                                 "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_get_property(val double) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS double " +
                                 "LANGUAGE JAVA\n" +
                                 "AS 'System.getProperty(\"foo.bar.baz\"); return 0d;';"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        }

        String[] cfnSources =
        { "try { Class.forName(\"" + JavaDriverUtils.class.getName() + "\"); } catch (Exception e) { throw new RuntimeException(e); } return 0d;",
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
                                    '}'},
        {"org.apache.cassandra.utils.vint.VIntCoding",
         "try {" +
         "     org.apache.cassandra.utils.vint.VIntCoding.computeUnsignedVIntSize(0L); return 0d;" +
         "} catch (Exception t) {" +
         "     throw new RuntimeException(t);" +
         '}'},
        // These APIs fall under allowed package prefixes but expose processes, classes, or native memory.
        {"java.lang.ProcessHandle", "java.lang.ProcessHandle.current(); return 0d;"},
        {"java.lang.StackWalker",   "java.lang.StackWalker.getInstance(); return 0d;"},
        {"java.lang.foreign.Linker","java.lang.foreign.Linker.nativeLinker(); return 0d;"},
        {"java.lang.classfile.ClassFile","java.lang.classfile.ClassFile.of(); return 0d;"} // JDK 24+
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

        // Class must resolve Module. Test the verifier rules that block Module and ModuleLayer calls.
        String[][] moduleApiSources =
        {
        {"java.lang.Class.getModule",   "java.lang.Integer.class.getModule(); return 0d;"},
        {"java.lang.ModuleLayer.boot",  "java.lang.ModuleLayer.boot(); return 0d;"}
        };
        for (String[] moduleApiSource : moduleApiSources)
            assertInvalidMessage("Java UDF validation failed: [call to " + moduleApiSource[0] + "()]",
                                 "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_module_access(val double) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS double " +
                                 "LANGUAGE JAVA\n" +
                                 "AS '" + moduleApiSource[1] + "';");
    }

    private static void assertAccessControlException(String script, FunctionExecutionException e)
    {
        for (Throwable t = e; t != null && t != t.getCause(); t = t.getCause())
            if (t instanceof AccessControlException)
                return;
        Assert.fail("no AccessControlException for " + script + " (got " + e + ')');
    }

    /**
     * Confirms that the byte-code sandbox rejects restricted System methods when a function is created.
     * It also confirms that timing and array methods remain available.
     */
    @Test
    public void testSandboxBlocksDangerousSystemMethods() throws Throwable
    {
        Assume.assumeFalse("legacy SecurityManager mechanism in use; covered by testSecurityPermissions",
                           ThreadAwareSecurityManager.useSecurityManager());

        String[][] methodAndSource =
        {
        {"exit",                "System.exit(1); return 0d;"},
        {"setProperty",         "System.setProperty(\"foo\", \"bar\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getProperty",         "System.getProperty(\"foo\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getenv",              "System.getenv(\"PATH\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"loadLibrary",         "System.loadLibrary(\"foo\"); return 0d;"},
        {"setSecurityManager",  "System.setSecurityManager(null); return 0d;"}
        };

        for (String[] ms : methodAndSource)
            assertInvalidMessage("Java UDF validation failed: [call to java.lang.System." + ms[0] + "()]",
                                 "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_system_" + ms[0].toLowerCase() + "(val double) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS double " +
                                 "LANGUAGE JAVA\n" +
                                 "AS '" + ms[1] + "';");

        // Test property aliases and the system logger factory.
        int alias = 0;
        String[][] aliasSources =
        {
        {"java.lang.System.getLogger",   "System.getLogger(\"x\"); return 0d;"},
        {"java.lang.System$LoggerFinder.getLoggerFinder", "System.LoggerFinder.getLoggerFinder(); return 0d;"},
        {"java.lang.Integer.getInteger", "Integer.getInteger(\"x\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"java.lang.Long.getLong",       "Long.getLong(\"x\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"java.lang.Boolean.getBoolean", "Boolean.getBoolean(\"x\"); return 0d;"} // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        };
        for (String[] as : aliasSources)
            assertInvalidMessage("Java UDF validation failed: [call to " + as[0] + "()]",
                                 "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_alias_" + (alias++) + "(val double) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS double " +
                                 "LANGUAGE JAVA\n" +
                                 "AS '" + as[1] + "';");

        // Timing methods remain available to UDFs.
        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA\n" +
                                      "AS 'return (double) (System.nanoTime() - System.currentTimeMillis());';");
        Assert.assertNotNull(fName);
    }

    /** Confirms that a UDF cannot emit a second class file that the verifier would not inspect. */
    @Test
    public void testRejectsAdditionalClasses() throws Throwable
    {
        // An anonymous Object subclass emits a second class file without using a blocked API.
        assertInvalidMessage("the function must not declare additional classes",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".invalid_inner_class(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return (double) new Object(){}.hashCode();';");
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

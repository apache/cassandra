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

import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;

import com.datastax.driver.core.ResultSet;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.security.UDFSecurityMechanism;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.Throwables;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

// UDF bodies below name the methods that blockSystemPropertyUsage looks for
// checkstyle: suppress below 'blockSystemPropertyUsage'
/**
 * Java UDFs that try to reach class loaders, the host or JVM-wide state, checked against the selected UDF sandbox.
 * {@link UFSandboxBypassBytecodeTest} runs them with the bytecode sandbox.
 */
public class UFSandboxBypassTest extends CQLTester
{
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Before
    public void createData()
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (1, 1)");
    }

    private static boolean bytecode()
    {
        return UDFSecurityMechanism.current() == UDFSecurityMechanism.BYTECODE;
    }

    private String select(String body) throws Throwable
    {
        return "SELECT " + createJavaFunction("double", "text", body) + "(dval) FROM %s WHERE key = 1";
    }

    private static String createRejected(String body)
    {
        return "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".rejected(val double) CALLED ON NULL INPUT " +
               "RETURNS text LANGUAGE java AS $$" + body + "$$";
    }

    private void assertRejected(String expected, String body) throws Throwable
    {
        assertInvalidMessage(expected, createRejected(body));
    }

    private void assertFailsWhenRun(Class<? extends Throwable> cause, String body) throws Throwable
    {
        String select = select(body);
        assertThatThrownBy(() -> execute(select))
        .isInstanceOf(FunctionExecutionException.class)
        .matches(e -> Throwables.anyCauseMatches(e, cause::isInstance), "caused by " + cause.getName());
    }

    /** Rejected by the bytecode sandbox, or denied by the Security Manager when run. */
    private void assertDenied(String bytecodeError, String body) throws Throwable
    {
        if (bytecode())
            assertRejected(bytecodeError, body);
        else
            assertFailsWhenRun(AccessControlException.class, body);
    }

    @Test
    public void testClassLoaderAccess() throws Throwable
    {
        assertRejected("call to java.lang.Module.getClassLoader()",
                       "return String.valueOf(getClass().getModule().getClassLoader());");
        assertRejected("call to java.lang.ClassLoader.getParent()",
                       "ClassLoader.getPlatformClassLoader().getParent(); return \"\";");
        assertRejected("call to java.lang.ClassLoader.getName()",
                       "return ClassLoader.getPlatformClassLoader().getName();");
        assertRejected("call to java.lang.ModuleLayer.boot()",
                       "return String.valueOf(ModuleLayer.boot().findLoader(\"java.sql\"));");
    }

    @Test
    public void testReflection() throws Throwable
    {
        assertRejected("StackWalker cannot be resolved",
                       "return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).getCallerClass().getName();");

        assertRejected("call to java.lang.Class.newInstance()",
                       "try { \"x\".getClass().newInstance(); } catch (Exception e) { } return \"\";");

        String moduleDescriptor = "return String.valueOf(Object.class.getModule().getDescriptor().packages().size());";
        if (bytecode())
            assertRejected("call to java.lang.Class.getModule()", moduleDescriptor);
        else
            execute(select(moduleDescriptor)); // not restricted by the Security Manager
    }

    @Test
    public void testProcessAccess() throws Throwable
    {
        assertRejected("ProcessHandle cannot be resolved",
                       "return ProcessHandle.current().info().commandLine().orElse(\"\");");
    }

    @Test
    public void testJvmDefaults() throws Throwable
    {
        String tz = TimeZone.getDefault().getID();
        Locale locale = Locale.getDefault();
        Locale formatLocale = Locale.getDefault(Locale.Category.FORMAT);
        try
        {
            assertDenied("call to java.util.TimeZone.setDefault()",
                         "TimeZone.setDefault(TimeZone.getTimeZone(\"Asia/Tokyo\")); return \"\";");
            assertDenied("call to java.util.Locale.setDefault()",
                         "Locale.setDefault(Locale.GERMANY); return \"\";");
            assertDenied("call to java.util.Locale.setDefault()",
                         "Locale.setDefault(Locale.Category.FORMAT, Locale.GERMANY); return \"\";");

            assertEquals(tz, TimeZone.getDefault().getID());
            assertEquals(locale, Locale.getDefault());
            assertEquals(formatLocale, Locale.getDefault(Locale.Category.FORMAT));
            assertRows(execute(select("return String.format(\"%.3f\", Math.PI);")), row(String.format(formatLocale, "%.3f", Math.PI)));
        }
        finally
        {
            TimeZone.setDefault(TimeZone.getTimeZone(tz));
            Locale.setDefault(locale);
            Locale.setDefault(Locale.Category.FORMAT, formatLocale);
        }
    }

    @Test
    public void testSystemAccess() throws Throwable
    {
        assertDenied("call to java.lang.Integer.getInteger()", "return String.valueOf(Integer.getInteger(\"x\"));");
        assertDenied("call to java.lang.Long.getLong()", "return String.valueOf(Long.getLong(\"x\"));");
        assertDenied("call to java.lang.Boolean.getBoolean()", "return String.valueOf(Boolean.getBoolean(\"x\"));");
        assertDenied("call to java.lang.System.getenv()", "return System.getenv(\"HOME\");");
        assertDenied("call to java.lang.System.setProperty()", "System.setProperty(\"a\", \"b\"); return \"\";");
        assertDenied("call to java.lang.System.loadLibrary()", "System.loadLibrary(\"x\"); return \"\";");

        String exit = "System.exit(0); return \"\";";
        if (bytecode())
            assertRejected("call to java.lang.System.exit()", exit);
        else
            createJavaFunction("double", "text", exit); // never executed
    }

    @Test
    public void testCommonPool() throws Throwable
    {
        // not run with the Security Manager sandbox, which only denies it when the common pool starts a thread
        String body = "Arrays.parallelSort(new int[100000]); return \"\";";
        if (bytecode())
            assertRejected("call to java.util.Arrays.parallelSort()", body);
        else
            createJavaFunction("double", "text", body);
    }

    @Test
    public void testHostNameResolution() throws Throwable
    {
        // the inet codec parses address literals only, with either sandbox
        String codec = "org.apache.cassandra.cql3.functions.types.TypeCodec.inet()";
        for (String rejected : new String[]{ "localhost", "fe80::1%lo0", "1.2.3.4.5", "256.1", "[1.2.3.4]" })
            assertFailsWhenRun(InvalidTypeException.class, "return " + codec + ".format(" + codec + ".parse(\"'" + rejected + "'\"));");

        String[][] literals = { { "127.0.0.1", "127.0.0.1" },
                                { "127.1", "127.0.0.1" },
                                { "2130706433", "127.0.0.1" },
                                { "", "127.0.0.1" },
                                { "[::1]", "0:0:0:0:0:0:0:1" },
                                { "fe80::1%4", "fe80:0:0:0:0:0:0:1%4" },
                                { "[fe80::1%4]", "fe80:0:0:0:0:0:0:1%4" } };
        for (String[] literal : literals)
            assertRows(execute(select("return " + codec + ".format(" + codec + ".parse(\"'" + literal[0] + "'\"));")), row("'" + literal[1] + "'"));
    }

    @Test
    public void testCodecBuffersAreNotShared() throws Throwable
    {
        // UDFs can write to the buffers the codecs return
        execute(select("org.apache.cassandra.cql3.functions.types.TypeCodec.cboolean()" +
                       ".serializeNoBoxing(true, org.apache.cassandra.transport.ProtocolVersion.CURRENT).put(0, (byte) 0);" +
                       "return \"\";"));
        assertRows(execute("SELECT " + createJavaFunction("double", "boolean", "return true;") + "(dval) FROM %s WHERE key = 1"),
                   row(true));
    }

    @Test
    public void testArgumentBuffersAreNotShared() throws Throwable
    {
        // UDFs can write to the buffers of their arguments, here the state built from INITCOND
        String blobAggregate = createIncrementingAggregate("blob", "state", "0x01");
        String tupleAggregate = createIncrementingAggregate("tuple<blob, int>", "state.getBytes(0)", "(0x01, 0)");
        String listAggregate = createIncrementingAggregate("list<blob>", "state.get(0)", "[0x01]");

        ByteBuffer two = ByteBuffer.wrap(new byte[]{ 2 });
        for (int i = 0; i < 2; i++)
        {
            assertRows(execute("SELECT " + blobAggregate + "(dval) FROM %s"), row(two));
            assertRows(execute("SELECT " + tupleAggregate + "(dval) FROM %s"), row(tuple(two, 0)));
            assertRows(execute("SELECT " + listAggregate + "(dval) FROM %s"), row(list(two)));
        }
    }

    /**
     * Creates an aggregate over a double whose state function increments the first byte of {@code buffer} in its state.
     */
    private String createIncrementingAggregate(String stateType, String buffer, String initcond) throws Throwable
    {
        String function = createFunction(KEYSPACE, stateType + ", double",
                                         "CREATE FUNCTION %s(state " + stateType + ", val double) CALLED ON NULL INPUT RETURNS " + stateType +
                                         " LANGUAGE java AS $$ByteBuffer b = " + buffer + "; b.put(b.position(), (byte) (b.get(b.position()) + 1)); return state;$$");
        return createAggregate(KEYSPACE, "double",
                               "CREATE AGGREGATE %s(double) SFUNC " + shortFunctionName(function) + " STYPE " + stateType + " INITCOND " + initcond);
    }

    @Test
    public void testFatalExceptions() throws Throwable
    {
        // both sandboxes reject constructing these
        KillerForTests killer = new KillerForTests();
        JVMStabilityInspector.Killer original = JVMStabilityInspector.replaceKiller(killer);
        try
        {
            assertRejected("use of type org.apache.cassandra.exceptions.UnrecoverableIllegalStateException",
                           "if (val == null) throw new org.apache.cassandra.exceptions.UnrecoverableIllegalStateException(\"x\"); return \"\";");
            assertRejected("use of type java.lang.OutOfMemoryError",
                           "if (val == null) throw new OutOfMemoryError(\"x\"); return \"\";");
            assertRejected("use of type java.lang.StackOverflowError",
                           "if (val == null) throw new StackOverflowError(); return \"\";");
            assertRejected("call to java.lang.Class.newInstance()",
                           "try { throw (Error) OutOfMemoryError.class.newInstance(); } " +
                           "catch (InstantiationException | IllegalAccessException e) { return \"\"; }");

            String cyclicCause = "RuntimeException a = new RuntimeException(\"a\"); RuntimeException b = new RuntimeException(\"b\", a); " +
                                 "a.initCause(b); throw a;";
            if (bytecode())
            {
                assertRejected("call to java.lang.RuntimeException.initCause()", cyclicCause);
                assertRejected("call to java.lang.RuntimeException.addSuppressed()",
                               "RuntimeException a = new RuntimeException(\"a\"); a.addSuppressed(a); return \"\";");
                assertRejected("call to java.lang.RuntimeException.printStackTrace()",
                               "new RuntimeException(\"a\").printStackTrace(); return \"\";");
            }
            else
            {
                // the native protocol handler passes the error to JVMStabilityInspector
                String f = createJavaFunction("double", "text", cyclicCause);
                assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT), "RuntimeException: a",
                                          com.datastax.driver.core.exceptions.FunctionExecutionException.class,
                                          "SELECT " + f + "(dval) FROM %s WHERE key = 1");
            }
            assertFalse(killer.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(original);
        }
    }

    @Test
    public void testClassShape() throws Throwable
    {
        assertRejected("additional class declared",
                       "Object o = new Object() { public String toString() { return \"\"; } }; return o.toString();");
        assertRejected("additional class declared", "class L { } return \"\";");
        assertRejected("field declared: f", "return \"\"; } int f; String y() { return \"\";");
        assertRejected("not allowed method declared: y()", "return \"\"; } String y() { return \"\";");
    }

    /**
     * The constructor runs outside UDF threads, the Security Manager and the UDF timeouts.
     */
    @Test
    public void testInjectedInitializer() throws Throwable
    {
        // each body closes the UDF method and opens an instance initializer that has no method calls and no
        // instructions without operands
        String[] bodies = {
            // variables, a loop and an increment
            "return \"\"; } { int i = 50; while (i < 0) { i++; } ",
            // a static field read
            "return \"\"; } { Object o = java.math.BigInteger.ONE; if (o == null) { o = java.math.BigInteger.TEN; } ",
            // a multi-dimensional allocation
            "return \"\"; } { long[][] a = new long[30][30]; while (a == null) { } ",
            // a switch
            "return \"\"; } { int i = 50; switch (i) { case 10: i = 20; break; case 1000: i = 30; break; default: i = 40; } ",
            // a string constant
            "return \"\"; } { String s = \"x\"; if (s == null) { s = \"y\"; } ",
            // a try/catch block
            "return \"\"; } { Object o = java.math.BigInteger.ONE; try { o = java.math.BigInteger.TEN; } catch (RuntimeException e) { o = e; } " +
            "if (o == null) { o = java.math.BigInteger.ONE; } ",
        };
        List<String> accepted = new ArrayList<>();
        for (String body : bodies)
        {
            try
            {
                assertRejected("initializer declared", body);
            }
            catch (AssertionError e)
            {
                accepted.add(body + " -> " + e.getMessage());
            }
        }
        assertEquals(Collections.emptyList(), accepted);
    }

    /**
     * The Security Manager does not check the foreign function and memory API.
     */
    @Test
    public void testForeignMemoryAccess() throws Throwable
    {
        Assume.assumeTrue("java.lang.foreign is not available on Java " + Runtime.version().feature(),
                          ModuleLayer.boot().findModule("java.base").get().getPackages().contains("java.lang.foreign"));
        assertRejected("cannot be resolved",
                       "return String.valueOf(java.lang.foreign.MemorySegment.ofAddress(0L).address());");
        assertRejected("cannot be resolved",
                       "java.lang.foreign.Arena arena = java.lang.foreign.Arena.ofAuto(); return String.valueOf(arena);");
        assertRejected("cannot be resolved",
                       "return String.valueOf(java.lang.foreign.Linker.nativeLinker().defaultLookup().find(\"system\").isPresent());");
    }

    /**
     * With the Security Manager sandbox, a function the bytecode sandbox would reject is created with a warning.
     */
    @Test
    public void testBytecodeSandboxReport() throws Throwable
    {
        if (!bytecode())
        {
            String body = "new RuntimeException(\"a\").printStackTrace(); return \"\";";
            String error = "[call to java.lang.RuntimeException.printStackTrace()]";
            ResultSet rs = executeNet("CREATE FUNCTION " + KEYSPACE + ".sm_only(val double) CALLED ON NULL INPUT RETURNS text " +
                                      "LANGUAGE java AS $$" + body + "$$");
            assertWarningsContain(rs.getExecutionInfo().getWarnings(),
                                  "Function " + KEYSPACE + ".sm_only(double) would be rejected by the bytecode UDF sandbox " +
                                  "used on Java 24 and later: " + error);
            assertEquals(Collections.singletonList(KEYSPACE + ".sm_only(double): " + error),
                         UDFunction.rejectedByBytecodeSandbox(Schema.instance.getKeyspaceMetadata(KEYSPACE).userFunctions.udfs()));
        }

        ResultSet rs = executeNet("CREATE FUNCTION " + KEYSPACE + ".both_mechanisms(val double) CALLED ON NULL INPUT RETURNS text " +
                                  "LANGUAGE java AS $$return String.valueOf(val);$$");
        assertNoWarningContains(rs.getExecutionInfo().getWarnings(), "bytecode UDF sandbox");
    }

    /**
     * allow_extra_insecure_udfs only has an effect without UDF threads, with either sandbox.
     */
    @Test
    public void testAllowExtraInsecureUdfs() throws Throwable
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        boolean threads = conf.user_defined_functions_threads_enabled;
        boolean insecure = conf.allow_insecure_udfs;
        boolean extraInsecure = conf.allow_extra_insecure_udfs;
        String body = "return System.getProperty(\"user.dir\");";
        try
        {
            conf.allow_insecure_udfs = true;
            for (boolean withThreads : new boolean[]{ true, false })
            {
                for (boolean extra : new boolean[]{ false, true })
                {
                    conf.user_defined_functions_threads_enabled = withThreads;
                    conf.allow_extra_insecure_udfs = extra;
                    if (!withThreads && !extra)
                        assertRejected("System cannot be resolved", body); // name filter, both sandboxes
                    else if (!withThreads)
                        assertRows(execute(select(body)), row(System.getProperty("user.dir"))); // the opt-in, both sandboxes
                    else if (bytecode())
                        assertRejected("call to java.lang.System.getProperty()", body); // the flag has no effect
                    else
                        assertFailsWhenRun(AccessControlException.class, body); // the Security Manager still checks

                    // PrintStream is never resolvable
                    if (withThreads || extra)
                        assertRejected("java.io.PrintStream cannot be resolved", "System.setOut(null); return \"\";");
                }
            }
        }
        finally
        {
            conf.user_defined_functions_threads_enabled = threads;
            conf.allow_insecure_udfs = insecure;
            conf.allow_extra_insecure_udfs = extraInsecure;
        }
    }

    @Test
    public void testThreadlessExecution() throws Throwable
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        boolean threads = conf.user_defined_functions_threads_enabled;
        boolean insecure = conf.allow_insecure_udfs;
        String tz = TimeZone.getDefault().getID();
        conf.user_defined_functions_threads_enabled = false;
        conf.allow_insecure_udfs = true;
        try
        {
            String body = "TimeZone.setDefault(TimeZone.getTimeZone(\"Asia/Tokyo\")); return TimeZone.getDefault().getID();";
            if (bytecode())
            {
                // the bytecode sandbox does not depend on the thread
                assertRejected("call to java.util.TimeZone.setDefault()", body);
            }
            else
            {
                // the Security Manager only checks UDF threads, so this runs
                assertRows(execute(select(body)), row("Asia/Tokyo"));
            }
        }
        finally
        {
            conf.user_defined_functions_threads_enabled = threads;
            conf.allow_insecure_udfs = insecure;
            TimeZone.setDefault(TimeZone.getTimeZone(tz));
        }
    }
}

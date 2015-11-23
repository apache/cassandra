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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.UDFByteCodeVerifier;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallClone;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallComDatastax;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallFinalize;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallOrgApache;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithField;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithInitializer;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithInitializer2;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithInitializer3;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithStaticInitializer;
import org.apache.cassandra.cql3.validation.entities.udfverify.GoodClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronized;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithNotify;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithNotifyAll;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithWait;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithWaitL;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithWaitLI;

import static org.junit.Assert.assertEquals;

/**
 * Test the Java UDF byte code verifier.
 */
public class UFVerifierTest extends CQLTester
{
    @Test
    public void testByteCodeVerifier()
    {
        new UDFByteCodeVerifier().verify(readClass(GoodClass.class));
    }

    @Test
    public void testClassWithField()
    {
        assertEquals(new HashSet<>(Collections.singletonList("field declared: field")),
                     new UDFByteCodeVerifier().verify(readClass(ClassWithField.class)));
    }

    @Test
    public void testClassWithInitializer()
    {
        assertEquals(new HashSet<>(Arrays.asList("field declared: field",
                                                 "initializer declared")),
                     new UDFByteCodeVerifier().verify(readClass(ClassWithInitializer.class)));
    }

    @Test
    public void testClassWithInitializer2()
    {
        assertEquals(new HashSet<>(Arrays.asList("field declared: field",
                                                 "initializer declared")),
                     new UDFByteCodeVerifier().verify(readClass(ClassWithInitializer2.class)));
    }

    @Test
    public void testClassWithInitializer3()
    {
        assertEquals(new HashSet<>(Collections.singletonList("initializer declared")),
                     new UDFByteCodeVerifier().verify(readClass(ClassWithInitializer3.class)));
    }

    @Test
    public void testClassWithStaticInitializer()
    {
        assertEquals(new HashSet<>(Collections.singletonList("static initializer declared")),
                     new UDFByteCodeVerifier().verify(readClass(ClassWithStaticInitializer.class)));
    }

    @Test
    public void testUseOfSynchronized()
    {
        assertEquals(new HashSet<>(Collections.singletonList("use of synchronized")),
                     new UDFByteCodeVerifier().verify(readClass(UseOfSynchronized.class)));
    }

    @Test
    public void testUseOfSynchronizedWithNotify()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.notify()")),
                     new UDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithNotify.class)));
    }

    @Test
    public void testUseOfSynchronizedWithNotifyAll()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.notifyAll()")),
                     new UDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithNotifyAll.class)));
    }

    @Test
    public void testUseOfSynchronizedWithWait()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.wait()")),
                     new UDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithWait.class)));
    }

    @Test
    public void testUseOfSynchronizedWithWaitL()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.wait()")),
                     new UDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithWaitL.class)));
    }

    @Test
    public void testUseOfSynchronizedWithWaitI()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.wait()")),
                     new UDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithWaitLI.class)));
    }

    @Test
    public void testCallClone()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to java.lang.Object.clone()")),
                     new UDFByteCodeVerifier().verify(readClass(CallClone.class)));
    }

    @Test
    public void testCallFinalize()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to java.lang.Object.finalize()")),
                     new UDFByteCodeVerifier().verify(readClass(CallFinalize.class)));
    }

    @Test
    public void testCallComDatastax()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to com.datastax.driver.core.DataType.cint()")),
                     new UDFByteCodeVerifier().addDisallowedPackage("com/").verify(readClass(CallComDatastax.class)));
    }

    @Test
    public void testCallOrgApache()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to org.apache.cassandra.config.DatabaseDescriptor.getClusterName()")),
                     new UDFByteCodeVerifier().addDisallowedPackage("org/").verify(readClass(CallOrgApache.class)));
    }

    @SuppressWarnings("resource")
    private static byte[] readClass(Class<?> clazz)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        URL res = clazz.getClassLoader().getResource(clazz.getName().replace('.', '/') + ".class");
        assert res != null;
        try (InputStream input = res.openConnection().getInputStream())
        {
            int i;
            while ((i = input.read()) != -1)
                out.write(i);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInvalidByteCodeUDFs() throws Throwable
    {
        assertInvalidByteCode("try\n" +
                              "{\n" +
                              "    clone();\n" +
                              "}\n" +
                              "catch (CloneNotSupportedException e)\n" +
                              "{\n" +
                              "    throw new RuntimeException(e);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.clone()]");
        assertInvalidByteCode("try\n" +
                              "{\n" +
                              "    finalize();\n" +
                              "}\n" +
                              "catch (Throwable e)\n" +
                              "{\n" +
                              "    throw new RuntimeException(e);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.finalize()]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    Object field;\n" +
                              '\n' +
                              "    {", "Java UDF validation failed: [field declared: field]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    final Object field;\n" +
                              '\n' +
                              "    {\n" +
                              "field = new Object();", "Java UDF validation failed: [field declared: field, initializer declared]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    Object field = new Object();\n" +
                              '\n' +
                              "    {\n" +
                              "Math.sin(1d);", "Java UDF validation failed: [field declared: field, initializer declared]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    {\n" +
                              "Math.sin(1d);", "Java UDF validation failed: [initializer declared]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    static\n" +
                              "    {\n" +
                              "Math.sin(1d);", "Java UDF validation failed: [static initializer declared]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    Math.sin(1d);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    notify();\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.notify(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    notifyAll();\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.notifyAll(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    try\n" +
                              "    {\n" +
                              "        wait();\n" +
                              "    }\n" +
                              "    catch (InterruptedException e)\n" +
                              "    {\n" +
                              "        throw new RuntimeException(e);\n" +
                              "    }\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.wait(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    try\n" +
                              "    {\n" +
                              "        wait(1000L);\n" +
                              "    }\n" +
                              "    catch (InterruptedException e)\n" +
                              "    {\n" +
                              "        throw new RuntimeException(e);\n" +
                              "    }\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.wait(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    try\n" +
                              "    {\n" +
                              "        wait(1000L, 100);\n" +
                              "    }\n" +
                              "    catch (InterruptedException e)\n" +
                              "    {\n" +
                              "        throw new RuntimeException(e);\n" +
                              "    }\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.wait(), use of synchronized]");
        assertInvalidByteCode("try {" +
                              "     java.nio.ByteBuffer.allocateDirect(123); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.nio.ByteBuffer.allocateDirect()]");
        assertInvalidByteCode("try {" +
                              "     java.net.InetAddress.getLocalHost(); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.InetAddress.getLocalHost()]");
        assertInvalidByteCode("try {" +
                              "     java.net.InetAddress.getAllByName(\"localhost\"); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.InetAddress.getAllByName()]");
        assertInvalidByteCode("try {" +
                              "     java.net.Inet4Address.getByName(\"127.0.0.1\"); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.Inet4Address.getByName()]");
        assertInvalidByteCode("try {" +
                              "     java.net.Inet6Address.getByAddress(new byte[]{127,0,0,1}); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.Inet6Address.getByAddress()]");
        assertInvalidByteCode("try {" +
                              "     java.net.NetworkInterface.getNetworkInterfaces(); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.NetworkInterface.getNetworkInterfaces()]");
    }

    private void assertInvalidByteCode(String body, String error) throws Throwable
    {
        assertInvalidMessage(error,
                             "CREATE FUNCTION " + KEYSPACE + ".mustBeInvalid ( input double ) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE java AS $$" + body + "$$");
    }
}

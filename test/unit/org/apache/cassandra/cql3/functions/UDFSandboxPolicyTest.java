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

package org.apache.cassandra.cql3.functions;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.BeforeClass;
import org.junit.Test;
import org.objectweb.asm.Type;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UDFSandboxPolicyTest
{
    private static final UDFSandboxPolicy POLICY = UDFSandboxPolicy.STANDARD;
    private static final String SURFACE_RESOURCE = "udf-sandbox-surface.txt";
    private static final String EXTRA_INSECURE_PREFIX = "extra-insecure ";
    private static final String OWNER_PREFIX = "owner ";
    // excluded members that only exist on newer JDKs
    private static final Set<String> NEWER_JDK_MEMBERS = Collections.singleton("java.math.BigInteger.parallelMultiply");

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testNamedMembersExist()
    {
        for (UDFSandboxPolicy.Entry entry : POLICY.entries().values())
        {
            Set<String> publicMembers = new HashSet<>();
            for (Member m : entry.type.getMethods())
                publicMembers.add(m.getName());
            for (Member m : entry.type.getFields())
                publicMembers.add(m.getName());
            if (entry.type.getConstructors().length > 0)
                publicMembers.add("<init>");
            for (String name : entry.names)
                assertTrue(entry.type.getName() + '.' + name + " does not exist",
                           publicMembers.contains(name) || NEWER_JDK_MEMBERS.contains(entry.type.getName() + '.' + name));
        }
    }

    @Test
    public void testListedClassesPassTheNameFilter()
    {
        for (String internalName : POLICY.entries().keySet())
            assertTrue(internalName, UDFunction.secureResource(internalName + ".class"));
    }

    /**
     * The JVM verifier loads supertypes named in stack map frames through the UDF class loader, which applies the
     * name filter, so they must pass it.
     */
    @Test
    public void testSupertypesPassTheNameFilter()
    {
        // TypeToken's superclass, the reflection interfaces of Class and an internal interface of ProtocolVersion are
        // never a merge type, because no other listed type extends or implements them.
        Set<String> exempt = new HashSet<>(Arrays.asList("com/google/common/reflect/TypeCapture",
                                                         "java/lang/reflect/GenericDeclaration",
                                                         "java/lang/reflect/AnnotatedElement",
                                                         "java/lang/reflect/Type",
                                                         "java/lang/invoke/TypeDescriptor$OfField",
                                                         "java/lang/invoke/TypeDescriptor",
                                                         "org/apache/cassandra/cql3/FunctionContext$NoTimeOrQueryFunctionContext",
                                                         "org/apache/cassandra/cql3/FunctionContext$NoTimeFunctionContext",
                                                         "org/apache/cassandra/cql3/FunctionContext",
                                                         // interfaces of the summary statistics; see UFSandboxAllowedTest for a
                                                         // merge of two of them, which the JVM verifier accepts
                                                         "java/util/function/IntConsumer",
                                                         "java/util/function/LongConsumer",
                                                         "java/util/function/DoubleConsumer"));
        for (UDFSandboxPolicy.Entry entry : POLICY.entries().values())
        {
            Deque<Class<?>> todo = new ArrayDeque<>();
            todo.add(entry.type);
            while (!todo.isEmpty())
            {
                Class<?> c = todo.poll();
                String name = Type.getInternalName(c);
                if (!exempt.contains(name))
                    assertTrue(name + " (supertype of " + entry.type.getName() + ')', UDFunction.secureResource(name + ".class"));
                if (c.getSuperclass() != null)
                    todo.add(c.getSuperclass());
                todo.addAll(Arrays.asList(c.getInterfaces()));
            }
        }
    }

    @Test
    public void testTypes()
    {
        assertTrue(POLICY.isTypeAllowed("java/lang/String"));
        assertTrue(POLICY.isTypeAllowed("[Ljava/lang/String;"));
        assertTrue(POLICY.isTypeAllowed("[[I"));
        assertTrue(POLICY.isTypeAllowed("java/net/InetAddress"));
        assertFalse(POLICY.isTypeAllowed("java/lang/Runtime"));
        assertFalse(POLICY.isTypeAllowed("[Ljava/lang/Thread;"));
        assertFalse(POLICY.isTypeAllowed("java/lang/ClassLoader"));
        assertFalse(POLICY.isTypeAllowed("org/apache/cassandra/exceptions/UnrecoverableIllegalStateException"));
    }

    @Test
    public void testMethodResolution()
    {
        // Object declares wait and does not grant it, whatever the receiver
        assertFalse(POLICY.isMethodAllowed("java/lang/Object", "wait", "()V"));
        assertTrue(POLICY.isMethodAllowed("java/lang/Object", "hashCode", "()I"));
        // a static method inherited by subclasses is checked against its declaring class
        assertFalse(POLICY.isMethodAllowed("java/net/Inet4Address", "getByName", "(Ljava/lang/String;)Ljava/net/InetAddress;"));
        assertFalse(POLICY.isMethodAllowed("java/net/InetAddress", "getByName", "(Ljava/lang/String;)Ljava/net/InetAddress;"));
        assertTrue(POLICY.isMethodAllowed("java/net/Inet4Address", "getHostAddress", "()Ljava/lang/String;"));
        assertFalse(POLICY.isMethodAllowed("java/util/SimpleTimeZone", "setDefault", "(Ljava/util/TimeZone;)V"));
        assertTrue(POLICY.isMethodAllowed("java/util/SimpleTimeZone", "getID", "()Ljava/lang/String;"));
        // inherited from a listed class that grants it
        assertTrue(POLICY.isMethodAllowed("java/lang/NumberFormatException", "getMessage", "()Ljava/lang/String;"));
        assertTrue(POLICY.isMethodAllowed("java/lang/NumberFormatException", "<init>", "(Ljava/lang/String;)V"));
        assertFalse(POLICY.isMethodAllowed("java/lang/NumberFormatException", "fillInStackTrace", "()Ljava/lang/Throwable;"));
        // inherited from an unlisted superclass of an "all" class
        assertTrue(POLICY.isMethodAllowed("java/lang/StringBuilder", "length", "()I"));
        assertTrue(POLICY.isMethodAllowed("java/lang/StringBuilder", "append", "(I)Ljava/lang/StringBuilder;"));
        assertTrue(POLICY.isMethodAllowed("java/util/ArrayList", "<init>", "()V"));
        // Object's methods through an interface
        assertTrue(POLICY.isMethodAllowed("java/util/Iterator", "toString", "()Ljava/lang/String;"));
        // descriptors with unlisted types are rejected
        assertFalse(POLICY.isMethodAllowed("java/lang/String", "chars", "()Ljava/util/stream/IntStream;"));
        assertFalse(POLICY.isMethodAllowed("java/lang/System", "getProperties", "()Ljava/util/Properties;"));
        assertFalse(UDFSandboxPolicy.EXTRA_INSECURE.isMethodAllowed("java/lang/System", "getProperties", "()Ljava/util/Properties;"));
        // exclusions
        assertFalse(POLICY.isMethodAllowed("java/lang/Integer", "getInteger", "(Ljava/lang/String;)Ljava/lang/Integer;"));
        assertFalse(POLICY.isMethodAllowed("java/util/Arrays", "parallelSort", "([I)V"));
        // the inet codec's parse accepts address literals only, so codec parsing is allowed
        assertTrue(POLICY.isMethodAllowed("org/apache/cassandra/cql3/functions/types/TypeCodec", "parse", "(Ljava/lang/String;)Ljava/lang/Object;"));
        assertFalse(POLICY.isMethodAllowed("java/math/BigInteger", "parallelMultiply", "(Ljava/math/BigInteger;)Ljava/math/BigInteger;"));
        assertTrue(POLICY.isMethodAllowed("java/util/IntSummaryStatistics", "accept", "(I)V"));
        assertFalse(POLICY.isMethodAllowed("java/util/IntSummaryStatistics", "andThen", "(Ljava/util/function/IntConsumer;)Ljava/util/function/IntConsumer;"));
        assertTrue(POLICY.isMethodAllowed("org/apache/cassandra/cql3/functions/types/TypeCodec", "format", "(Ljava/lang/Object;)Ljava/lang/String;"));
        // System: getProperty needs allow_extra_insecure_udfs
        assertTrue(POLICY.isMethodAllowed("java/lang/System", "currentTimeMillis", "()J"));
        assertFalse(POLICY.isMethodAllowed("java/lang/System", "getProperty", "(Ljava/lang/String;)Ljava/lang/String;"));
        assertTrue(UDFSandboxPolicy.EXTRA_INSECURE.isMethodAllowed("java/lang/System", "getProperty", "(Ljava/lang/String;)Ljava/lang/String;"));
        // type-only and unlisted
        assertFalse(POLICY.isMethodAllowed("java/lang/OutOfMemoryError", "<init>", "()V"));
        assertFalse(POLICY.isMethodAllowed("java/lang/Runtime", "getRuntime", "()Ljava/lang/Runtime;"));
        // a member that does not exist
        assertFalse(POLICY.isMethodAllowed("java/lang/String", "noSuchMethod", "()V"));
    }

    @Test
    public void testFields()
    {
        assertTrue(POLICY.isFieldAllowed("java/math/RoundingMode", "HALF_UP", "Ljava/math/RoundingMode;"));
        assertTrue(POLICY.isFieldAllowed("org/apache/cassandra/transport/ProtocolVersion", "CURRENT", "Lorg/apache/cassandra/transport/ProtocolVersion;"));
        assertFalse(POLICY.isFieldAllowed("java/lang/System", "out", "Ljava/io/PrintStream;"));
        assertFalse(UDFSandboxPolicy.EXTRA_INSECURE.isFieldAllowed("java/lang/System", "out", "Ljava/io/PrintStream;"));
        assertFalse(POLICY.isFieldAllowed("java/lang/Integer", "MAX_VALUE", "J"));
        assertTrue(POLICY.isFieldAllowed("java/lang/Integer", "MAX_VALUE", "I"));
    }

    /**
     * Fails when a JDK or dependency change makes another member usable from UDFs, until {@value #SURFACE_RESOURCE}
     * or the policy is updated. The resource header describes the format.
     */
    @Test
    public void testAdmittedMembersMatchRecordedSurface() throws IOException
    {
        Set<String> recorded;
        try (InputStream in = UDFSandboxPolicyTest.class.getClassLoader().getResourceAsStream(SURFACE_RESOURCE))
        {
            assertNotNull(SURFACE_RESOURCE, in);
            recorded = parseSurface(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        Set<String> unexpected = new TreeSet<>(currentSurface());
        unexpected.removeAll(recorded);
        assertEquals("Unexpected members accessible to UDFs; update the policy or " + SURFACE_RESOURCE,
                     Collections.emptySet(), unexpected);
    }

    @Test
    public void testSurfaceFollowsPolicyResolution()
    {
        Set<String> standard = admittedSurface(UDFSandboxPolicy.STANDARD);
        // an Object method through an interface owner resolves to Object
        assertTrue(standard.contains("java/util/Iterator toString()Ljava/lang/String; java/lang/Object"));
        // inherited declarations are recorded with their declaring class, listed or not
        assertTrue(standard.contains("java/lang/NumberFormatException getMessage()Ljava/lang/String; java/lang/Throwable"));
        assertTrue(standard.contains("java/util/ArrayList containsAll(Ljava/util/Collection;)Z java/util/AbstractCollection"));
        assertTrue(standard.contains("java/util/SimpleTimeZone getTimeZone(Ljava/lang/String;)Ljava/util/TimeZone; java/util/TimeZone"));
        assertTrue(standard.contains("java/math/RoundingMode HALF_UP:Ljava/math/RoundingMode; java/math/RoundingMode"));

        // the extra-insecure policy only widens java.lang.System, and still rejects descriptors with unlisted types
        Set<String> extra = admittedSurface(UDFSandboxPolicy.EXTRA_INSECURE);
        assertTrue(extra.containsAll(standard));
        Set<String> delta = new TreeSet<>(extra);
        delta.removeAll(standard);
        assertFalse(delta.isEmpty());
        for (String row : delta)
            assertTrue(row, row.startsWith("java/lang/System ") && row.endsWith(" java/lang/System"));
        assertTrue(delta.contains("java/lang/System getProperty(Ljava/lang/String;)Ljava/lang/String; java/lang/System"));
        assertFalse(delta.toString(), delta.stream().anyMatch(r -> r.contains("getProperties") || r.contains("setOut")));
    }

    /**
     * Merges the surface of the running JDK into the file at {@code args[0]}.
     */
    public static void main(String[] args) throws IOException
    {
        Path file = Path.of(args[0]);
        String text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        StringBuilder header = new StringBuilder();
        for (String line : text.split("\n"))
            if (line.startsWith("#"))
                header.append(line).append('\n');
        Set<String> rows = parseSurface(text);
        rows.addAll(currentSurface());
        Files.write(file, (header + formatSurface(rows)).getBytes(StandardCharsets.UTF_8));
    }

    /** Rows {@code [extra-insecure ]<declaring class> <member>} and {@code [extra-insecure ]owner <owner> <declaring class>}. */
    private static Set<String> currentSurface()
    {
        Set<String> standard = surfaceRows(admittedSurface(UDFSandboxPolicy.STANDARD));
        Set<String> rows = new TreeSet<>(standard);
        for (String row : surfaceRows(admittedSurface(UDFSandboxPolicy.EXTRA_INSECURE)))
            if (!standard.contains(row))
                rows.add(EXTRA_INSECURE_PREFIX + row);
        return rows;
    }

    /**
     * Maps rows {@code <owner> <member> <declaring class>} to one row per declared member and one per owner and
     * declaring class pair.
     */
    private static Set<String> surfaceRows(Set<String> admitted)
    {
        Set<String> rows = new TreeSet<>();
        for (String row : admitted)
        {
            String[] parts = row.split(" ");
            rows.add(parts[2] + ' ' + parts[1]);
            rows.add(OWNER_PREFIX + parts[0] + ' ' + parts[2]);
        }
        return rows;
    }

    /**
     * Enumerates candidates the way {@link UDFSandboxPolicy} resolves them: public methods, constructors and fields of
     * each listed class, plus Object's methods for interfaces, as rows {@code <owner> <member> <declaring class>}.
     */
    private static Set<String> admittedSurface(UDFSandboxPolicy policy)
    {
        Set<String> admitted = new TreeSet<>();
        for (Map.Entry<String, UDFSandboxPolicy.Entry> e : policy.entries().entrySet())
        {
            String owner = e.getKey();
            Class<?> c = e.getValue().type;
            Set<String> members = new TreeSet<>();
            for (Method m : c.getMethods())
                members.add(m.getName() + Type.getMethodDescriptor(m));
            if (c.isInterface())
                for (Method m : Object.class.getMethods())
                    members.add(m.getName() + Type.getMethodDescriptor(m));
            for (Constructor<?> k : c.getConstructors())
                members.add("<init>" + Type.getConstructorDescriptor(k));
            for (String member : members)
            {
                int paren = member.indexOf('(');
                String name = member.substring(0, paren);
                String desc = member.substring(paren);
                if (policy.isMethodAllowed(owner, name, desc))
                    admitted.add(owner + ' ' + member + ' ' + policy.declaringClass(owner, name, desc));
            }
            for (Field f : c.getFields())
            {
                String desc = Type.getDescriptor(f.getType());
                if (policy.isFieldAllowed(owner, f.getName(), desc))
                    admitted.add(owner + ' ' + f.getName() + ':' + desc + ' ' + policy.declaringClass(owner, f.getName(), desc));
            }
        }
        return admitted;
    }

    private static String formatSurface(Set<String> rows)
    {
        StringBuilder out = new StringBuilder();
        for (String row : new TreeSet<>(rows))
            out.append(row).append('\n');
        return out.toString();
    }

    private static Set<String> parseSurface(String text)
    {
        Set<String> rows = new HashSet<>();
        for (String line : text.split("\n"))
            if (!line.isEmpty() && !line.startsWith("#"))
                rows.add(line);
        return rows;
    }
}

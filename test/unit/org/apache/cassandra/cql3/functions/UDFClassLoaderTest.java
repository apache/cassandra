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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.BeforeClass;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Checks the UDF class name filter when the JVM links a UDF class. The classes here bypass the bytecode verifier.
 */
public class UDFClassLoaderTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testLinkingToDeniedClassFails() throws Exception
    {
        Method run = define("Linked1", "java/lang/Runtime", "getRuntime", "()Ljava/lang/Runtime;").getMethod("run");
        assertThatThrownBy(() -> run.invoke(null))
        .isInstanceOf(InvocationTargetException.class)
        .cause()
        .isInstanceOf(NoClassDefFoundError.class)
        .hasMessageContaining("java/lang/Runtime");
    }

    @Test
    public void testLinkingToCassandraInternalsFails() throws Exception
    {
        Method run = define("Linked2", "org/apache/cassandra/config/DatabaseDescriptor", "getClusterName", "()Ljava/lang/String;").getMethod("run");
        assertThatThrownBy(() -> run.invoke(null))
        .isInstanceOf(InvocationTargetException.class)
        .cause()
        .isInstanceOf(NoClassDefFoundError.class)
        .hasMessageContaining("org/apache/cassandra/config/DatabaseDescriptor");
    }

    @Test
    public void testForNameThroughUdfLoader() throws Exception
    {
        ClassLoader loader = new JavaBasedUDFunction.EcjTargetClassLoader();
        assertEquals(String.class, Class.forName("java.lang.String", false, loader));
        for (String denied : new String[]{ "java.lang.Runtime", "java.io.File", "java.util.concurrent.Executors",
                                           "org.apache.cassandra.service.StorageService" })
        {
            assertThatThrownBy(() -> Class.forName(denied, false, loader)).isInstanceOf(ClassNotFoundException.class);
        }
    }

    /**
     * Defines {@code public class <name> { public static Object run() { return <owner>.<method>(); } }}.
     */
    private static Class<?> define(String simpleName, String owner, String method, String desc) throws Exception
    {
        String name = "org/apache/cassandra/cql3/udf/gen/" + simpleName;
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER, name, null, "java/lang/Object", null);
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "run", "()Ljava/lang/Object;", null, null);
        mv.visitCode();
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, owner, method, desc, false);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
        cw.visitEnd();

        JavaBasedUDFunction.EcjTargetClassLoader loader = new JavaBasedUDFunction.EcjTargetClassLoader();
        String binaryName = name.replace('/', '.');
        loader.addClass(binaryName, cw.toByteArray());
        return Class.forName(binaryName, true, loader);
    }
}

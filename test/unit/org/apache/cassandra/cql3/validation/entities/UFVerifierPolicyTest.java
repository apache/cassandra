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

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import org.apache.cassandra.cql3.functions.UDFByteCodeVerifier;
import org.apache.cassandra.cql3.functions.UDFSandboxPolicy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies bytecode instructions that ECJ does not emit for UDF source.
 */
public class UFVerifierPolicyTest
{
    private static final String CLS = "org/apache/cassandra/cql3/udf/gen/p/C";
    private static final String IMPL = "x";
    private static final String IMPL_DESC = "()Ljava/lang/Object;";

    @Test
    public void testValidClass()
    {
        assertEquals(Collections.emptySet(), verifyPolicy(mv -> {
            mv.visitLdcInsn("x");
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/String", "length", "()I", false);
            mv.visitInsn(Opcodes.POP);
        }));
    }

    @Test
    public void testInvokedynamic()
    {
        Handle bsm = new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/StringConcatFactory", "makeConcatWithConstants",
                                "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/invoke/CallSite;",
                                false);
        Set<String> errors = verifyPolicy(mv -> {
            mv.visitInvokeDynamicInsn("makeConcatWithConstants", "()Ljava/lang/String;", bsm, "x");
            mv.visitInsn(Opcodes.POP);
        });
        assertTrue(errors.toString(), errors.contains("use of invokedynamic"));
    }

    @Test
    public void testUnsupportedConstants()
    {
        Handle handle = new Handle(Opcodes.H_INVOKESTATIC, "java/lang/System", "exit", "(I)V", false);
        assertTrue(verifyPolicy(mv -> { mv.visitLdcInsn(handle); mv.visitInsn(Opcodes.POP); })
                   .contains("use of unsupported constant"));
        assertTrue(verifyPolicy(mv -> { mv.visitLdcInsn(Type.getMethodType("()V")); mv.visitInsn(Opcodes.POP); })
                   .contains("use of unsupported constant"));
        assertTrue(verifyPolicy(mv -> { mv.visitLdcInsn(Type.getObjectType("java/lang/Runtime")); mv.visitInsn(Opcodes.POP); })
                   .contains("use of type java.lang.Runtime"));
        assertEquals(Collections.emptySet(), verifyPolicy(mv -> { mv.visitLdcInsn(Type.getObjectType("java/lang/String")); mv.visitInsn(Opcodes.POP); }));
    }

    @Test
    public void testUnlistedTypes()
    {
        assertTrue(verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Runtime");
            mv.visitInsn(Opcodes.POP);
        }).contains("use of type java.lang.Runtime"));
        assertTrue(verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Thread");
            mv.visitInsn(Opcodes.POP);
        }).contains("use of type java.lang.Thread"));
        assertTrue(verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMultiANewArrayInsn("[[Ljava/lang/ProcessBuilder;", 2);
            mv.visitInsn(Opcodes.POP);
        }).contains("use of type java.lang.ProcessBuilder"));
        assertTrue(verifyPolicy(mv -> {
            Label start = new Label();
            Label end = new Label();
            Label handler = new Label();
            Label done = new Label();
            mv.visitTryCatchBlock(start, end, handler, "java/io/FileNotFoundException");
            mv.visitLabel(start);
            mv.visitInsn(Opcodes.NOP);
            mv.visitLabel(end);
            mv.visitJumpInsn(Opcodes.GOTO, done);
            mv.visitLabel(handler);
            mv.visitInsn(Opcodes.POP);
            mv.visitLabel(done);
        }).contains("use of type java.io.FileNotFoundException"));
    }

    @Test
    public void testFieldAccess()
    {
        assertTrue(verifyPolicy(mv -> {
            mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
            mv.visitInsn(Opcodes.POP);
        }).contains("access to field java.lang.System.out"));
        // writes are never allowed
        assertTrue(verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ACONST_NULL);
            mv.visitFieldInsn(Opcodes.PUTSTATIC, "java/math/RoundingMode", "UP", "Ljava/math/RoundingMode;");
        }).contains("access to field java.math.RoundingMode.UP"));
        assertEquals(Collections.emptySet(), verifyPolicy(mv -> {
            mv.visitFieldInsn(Opcodes.GETSTATIC, "java/math/RoundingMode", "UP", "Ljava/math/RoundingMode;");
            mv.visitInsn(Opcodes.POP);
        }));
        // the UDF's own udfContext field can be read, also as super.udfContext; nothing else on the UDF class
        for (String owner : new String[]{ CLS, UDFByteCodeVerifier.JAVA_UDF_NAME })
        {
            assertEquals(Collections.emptySet(), verifyPolicy(mv -> {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, owner, "udfContext", "Lorg/apache/cassandra/cql3/functions/UDFContext;");
                mv.visitInsn(Opcodes.POP);
            }));
        }
    }

    @Test
    public void testCalls()
    {
        assertTrue(verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "exit", "(I)V", false);
        }).contains("call to java.lang.System.exit()"));
        // only the implementation method and decompose on the UDF class itself, and decompose on JavaUDF
        assertTrue(verifyPolicy(mv -> {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, CLS, "hashCode", "()I", false);
            mv.visitInsn(Opcodes.POP);
        }).contains("call to org.apache.cassandra.cql3.udf.gen.p.C.hashCode()"));
        assertTrue(verifyPolicy(mv -> {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, UDFByteCodeVerifier.JAVA_UDF_NAME, "hashCode", "()I", false);
            mv.visitInsn(Opcodes.POP);
        }).contains("call to org.apache.cassandra.cql3.functions.JavaUDF.hashCode()"));
        // arrays: clone and getClass only
        assertEquals(Collections.emptySet(), verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_INT);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "[I", "clone", "()Ljava/lang/Object;", false);
            mv.visitInsn(Opcodes.POP);
        }));
        assertTrue(verifyPolicy(mv -> {
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_INT);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "[I", "wait", "()V", false);
        }).contains("call to [I.wait()"));
    }

    @Test
    public void testClassShape()
    {
        Consumer<MethodVisitor> nothing = mv -> {};
        assertTrue(verify(new UDFByteCodeVerifier(UDFSandboxPolicy.STANDARD), nothing, true).contains("class declared as nest host"));
        // the implementation method must match by name and descriptor
        Set<String> errors = new UDFByteCodeVerifier().verify(CLS.replace('/', '.'), generate(nothing, false), IMPL, "()Ljava/lang/String;");
        assertTrue(errors.toString(), errors.contains("not allowed method declared: " + IMPL + IMPL_DESC));
    }

    private static Set<String> verifyPolicy(Consumer<MethodVisitor> body)
    {
        return verify(new UDFByteCodeVerifier(UDFSandboxPolicy.STANDARD), body, false);
    }

    private static Set<String> verify(UDFByteCodeVerifier verifier, Consumer<MethodVisitor> body, boolean nestMember)
    {
        return verifier.verify(CLS.replace('/', '.'), generate(body, nestMember), IMPL, IMPL_DESC);
    }

    /**
     * A class shaped like a generated UDF, whose implementation method runs {@code body} and returns null.
     */
    private static byte[] generate(Consumer<MethodVisitor> body, boolean nestMember)
    {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, CLS, null,
                 UDFByteCodeVerifier.JAVA_UDF_NAME, null);
        if (nestMember)
            cw.visitNestMember(CLS + "$1");

        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", UDFByteCodeVerifier.CTOR_SIG, null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, UDFByteCodeVerifier.JAVA_UDF_NAME, "<init>", UDFByteCodeVerifier.CTOR_SIG, false);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        mv = cw.visitMethod(Opcodes.ACC_PRIVATE, IMPL, IMPL_DESC, null, null);
        mv.visitCode();
        body.accept(mv);
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        cw.visitEnd();
        return cw.toByteArray();
    }
}

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Verifies Java UDF byte code.
 * Checks for disallowed method calls (e.g. {@code Object.finalize()}),
 * additional code in the constructor,
 * use of {@code synchronized} blocks,
 * too many methods.
 */
public final class UDFByteCodeVerifier
{

    public static final String JAVA_UDF_NAME = JavaUDF.class.getName().replace('.', '/');
    public static final String OBJECT_NAME = Object.class.getName().replace('.', '/');
    public static final String CTOR_SIG = "(Lcom/datastax/driver/core/TypeCodec;[Lcom/datastax/driver/core/TypeCodec;Lorg/apache/cassandra/cql3/functions/UDFContext;)V";

    private final Set<String> disallowedClasses = new HashSet<>();
    private final Multimap<String, String> disallowedMethodCalls = HashMultimap.create();
    private final List<String> disallowedPackages = new ArrayList<>();

    public UDFByteCodeVerifier()
    {
        addDisallowedMethodCall(OBJECT_NAME, "clone");
        addDisallowedMethodCall(OBJECT_NAME, "finalize");
        addDisallowedMethodCall(OBJECT_NAME, "notify");
        addDisallowedMethodCall(OBJECT_NAME, "notifyAll");
        addDisallowedMethodCall(OBJECT_NAME, "wait");
    }

    public UDFByteCodeVerifier addDisallowedClass(String clazz)
    {
        disallowedClasses.add(clazz);
        return this;
    }

    public UDFByteCodeVerifier addDisallowedMethodCall(String clazz, String method)
    {
        disallowedMethodCalls.put(clazz, method);
        return this;
    }

    public UDFByteCodeVerifier addDisallowedPackage(String pkg)
    {
        disallowedPackages.add(pkg);
        return this;
    }

    public Set<String> verify(String clsName, byte[] bytes)
    {
        String clsNameSl = clsName.replace('.', '/');
        Set<String> errors = new TreeSet<>(); // it's a TreeSet for unit tests
        ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM5)
        {
            public FieldVisitor visitField(int access, String name, String desc, String signature, Object value)
            {
                errors.add("field declared: " + name);
                return null;
            }

            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
            {
                if ("<init>".equals(name) && CTOR_SIG.equals(desc))
                {
                    if (Opcodes.ACC_PUBLIC != access)
                        errors.add("constructor not public");
                    // allowed constructor - JavaUDF(TypeCodec returnCodec, TypeCodec[] argCodecs)
                    return new ConstructorVisitor(errors);
                }
                if ("executeImpl".equals(name) && "(Lorg/apache/cassandra/transport/ProtocolVersion;Ljava/util/List;)Ljava/nio/ByteBuffer;".equals(desc))
                {
                    if (Opcodes.ACC_PROTECTED != access)
                        errors.add("executeImpl not protected");
                    // the executeImpl method - ByteBuffer executeImpl(ProtocolVersion protocolVersion, List<ByteBuffer> params)
                    return new ExecuteImplVisitor(errors);
                }
                if ("executeAggregateImpl".equals(name) && "(Lorg/apache/cassandra/transport/ProtocolVersion;Ljava/lang/Object;Ljava/util/List;)Ljava/lang/Object;".equals(desc))
                {
                    if (Opcodes.ACC_PROTECTED != access)
                        errors.add("executeAggregateImpl not protected");
                    // the executeImpl method - ByteBuffer executeImpl(ProtocolVersion protocolVersion, List<ByteBuffer> params)
                    return new ExecuteImplVisitor(errors);
                }
                if ("<clinit>".equals(name))
                {
                    errors.add("static initializer declared");
                }
                else
                {
                    errors.add("not allowed method declared: " + name + desc);
                    return new ExecuteImplVisitor(errors);
                }
                return null;
            }

            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
            {
                if (!JAVA_UDF_NAME.equals(superName))
                {
                    errors.add("class does not extend " + JavaUDF.class.getName());
                }
                if (access != (Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER))
                {
                    errors.add("class not public final");
                }
                super.visit(version, access, name, signature, superName, interfaces);
            }

            public void visitInnerClass(String name, String outerName, String innerName, int access)
            {
                if (clsNameSl.equals(outerName)) // outerName might be null, which is true for anonymous inner classes
                    errors.add("class declared as inner class");
                super.visitInnerClass(name, outerName, innerName, access);
            }
        };

        ClassReader classReader = new ClassReader(bytes);
        classReader.accept(classVisitor, ClassReader.SKIP_DEBUG);

        return errors;
    }

    private class ExecuteImplVisitor extends MethodVisitor
    {
        private final Set<String> errors;

        ExecuteImplVisitor(Set<String> errors)
        {
            super(Opcodes.ASM5);
            this.errors = errors;
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            if (disallowedClasses.contains(owner))
            {
                errorDisallowed(owner, name);
            }
            Collection<String> disallowed = disallowedMethodCalls.get(owner);
            if (disallowed != null && disallowed.contains(name))
            {
                errorDisallowed(owner, name);
            }
            if (!JAVA_UDF_NAME.equals(owner))
            {
                for (String pkg : disallowedPackages)
                {
                    if (owner.startsWith(pkg))
                        errorDisallowed(owner, name);
                }
            }
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        private void errorDisallowed(String owner, String name)
        {
            errors.add("call to " + owner.replace('/', '.') + '.' + name + "()");
        }

        public void visitInsn(int opcode)
        {
            switch (opcode)
            {
                case Opcodes.MONITORENTER:
                case Opcodes.MONITOREXIT:
                    errors.add("use of synchronized");
                    break;
            }
            super.visitInsn(opcode);
        }
    }

    private static class ConstructorVisitor extends MethodVisitor
    {
        private final Set<String> errors;

        ConstructorVisitor(Set<String> errors)
        {
            super(Opcodes.ASM5);
            this.errors = errors;
        }

        public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs)
        {
            errors.add("Use of invalid method instruction in constructor");
            super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            if (!(Opcodes.INVOKESPECIAL == opcode && JAVA_UDF_NAME.equals(owner) && "<init>".equals(name) && CTOR_SIG.equals(desc)))
            {
                errors.add("initializer declared");
            }
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        public void visitInsn(int opcode)
        {
            if (Opcodes.RETURN != opcode)
            {
                errors.add("initializer declared");
            }
            super.visitInsn(opcode);
        }
    }
}

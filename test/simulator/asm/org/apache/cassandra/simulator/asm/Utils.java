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

package org.apache.cassandra.simulator.asm;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;

import static org.apache.cassandra.simulator.asm.InterceptClasses.BYTECODE_VERSION;
import static org.objectweb.asm.Opcodes.ATHROW;
import static org.objectweb.asm.Opcodes.F_SAME1;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

public class Utils
{
    public static String toInternalName(Class<?> clazz)
    {
        return toInternalName(clazz.getName());
    }

    public static String toInternalName(String className)
    {
        return className.replace('.', '/');
    }

    public static String toPath(Class<?> clazz)
    {
        return toInternalName(clazz) + ".class";
    }

    public static byte[] readDefinition(Class<?> clazz) throws IOException
    {
        return readDefinition(toPath(clazz));
    }

    public static byte[] readDefinition(String path) throws IOException
    {
        byte[] bytes = new byte[1024];
        try (InputStream in = ClassLoader.getSystemResourceAsStream(path))
        {
            int count = 0;
            while (true)
            {
                int add = in.read(bytes, count, bytes.length - count);
                if (add < 0)
                    break;
                if (add == 0)
                    bytes = Arrays.copyOf(bytes, bytes.length * 2);
                count += add;
            }
            return Arrays.copyOf(bytes, count);
        }
    }

    /**
     * Generate a proxy method call, i.e. one whose only job is forwarding the parameters to a different method
     * (and perhaps within a superclass, or another class entirely if static) with the same signature but perhaps
     * different properties.
     */
    private static long visitProxyCall(MethodVisitor visitor, String calleeClassName, String calleeMethodName, String descriptor, int access, boolean isInstanceMethod, boolean isInterface)
    {
        Type[] argTypes = Type.getArgumentTypes(descriptor);
        Type returnType = Type.getReturnType(descriptor);

        int stackSize = argTypes.length;
        int localsSize = 0;
        if (isInstanceMethod)
        {
            visitor.visitIntInsn(Opcodes.ALOAD, 0);
            localsSize += 2;
            stackSize += 1;
        }

        int i = 1;
        for (Type type : argTypes)
        {
            int opcode;
            switch (type.getDescriptor().charAt(0))
            {
                case 'L':
                case '[':
                    opcode = Opcodes.ALOAD;
                    localsSize += 1;
                    break;
                case 'J':
                    opcode = Opcodes.LLOAD;
                    localsSize += 2;
                    break;
                case 'D':
                    opcode = Opcodes.DLOAD;
                    localsSize += 2;
                    break;
                case 'F':
                    opcode = Opcodes.FLOAD;
                    localsSize += 1;
                    break;
                default:
                    opcode = Opcodes.ILOAD;
                    localsSize += 1;
                    break;
            }
            visitor.visitIntInsn(opcode, i++);
        }

        int returnCode;
        switch (returnType.getDescriptor().charAt(0))
        {
            case 'L':
            case '[':
                returnCode = Opcodes.ARETURN;
                localsSize = Math.max(localsSize, 1);
                break;
            case 'J':
                returnCode = Opcodes.LRETURN;
                localsSize = Math.max(localsSize, 2);
                break;
            case 'D':
                returnCode = Opcodes.DRETURN;
                localsSize = Math.max(localsSize, 2);
                break;
            case 'F':
                returnCode = Opcodes.FRETURN;
                localsSize = Math.max(localsSize, 1);
                break;
            case 'V':
                returnCode = Opcodes.RETURN;
                break;
            default:
                returnCode = Opcodes.IRETURN;
                localsSize = Math.max(localsSize, 1);
                break;
        }

        int invokeCode;
        if (isInstanceMethod && (access & Opcodes.ACC_PRIVATE) != 0 || calleeMethodName.equals("<init>")) invokeCode = Opcodes.INVOKESPECIAL;
        else if (isInstanceMethod) invokeCode = Opcodes.INVOKEVIRTUAL;
        else invokeCode = Opcodes.INVOKESTATIC;

        visitor.visitMethodInsn(invokeCode, calleeClassName, calleeMethodName, descriptor, isInterface);

        return localsSize | (((long)stackSize) << 28) | (((long) returnCode) << 56);
    }

    /**
     * Generate a proxy method call, i.e. one whose only job is forwarding the parameters to a different method
     * (and perhaps within a superclass, or another class entirely if static) with the same signature but perhaps
     * different properties.
     */
    public static void generateProxyCall(MethodVisitor visitor, String calleeClassName, String calleeMethodName, String descriptor, int access, boolean isInstanceMethod, boolean isInterface)
    {
        Label start = new Label(), end = new Label();
        visitor.visitLabel(start);

        long sizesAndReturnCode = visitProxyCall(visitor, calleeClassName, calleeMethodName, descriptor, access, isInstanceMethod, isInterface);
        visitor.visitLabel(end);
        visitor.visitInsn((int)(sizesAndReturnCode >>> 56) & 0xff);
        visitor.visitMaxs((int)(sizesAndReturnCode >>> 28) & 0xfffffff, (int)(sizesAndReturnCode & 0xfffffff));
        visitor.visitEnd();
    }

    /**
     * Generate a proxy method call, i.e. one whose only job is forwarding the parameters to a different method
     * (and perhaps within a superclass, or another class entirely if static) with the same signature but perhaps
     * different properties.
     *
     * Invoke within a try/catch block, invoking the provided setup/cleanup instructions.
     * As designed these must not assign any local variables, and the catch block must be exception free.
     */
    public static void generateTryFinallyProxyCall(MethodVisitor visitor, String calleeClassName, String calleeMethodName, String descriptor, int access, boolean isInstanceMethod, boolean isInterface,
                                                   List<AbstractInsnNode> setup, List<AbstractInsnNode> cleanup)
    {
        Label startMethod = new Label(), startTry = new Label(), endTry = new Label(), startCatch = new Label(), endMethod = new Label();
        visitor.visitLabel(startMethod);
        visitor.visitTryCatchBlock(startTry, endTry, startCatch, null);
        setup.forEach(i -> i.accept(visitor));
        visitor.visitLabel(startTry);
        long sizesAndReturnCode = visitProxyCall(visitor, calleeClassName, calleeMethodName, descriptor, access, isInstanceMethod, isInterface);
        int returnCode = (int)(sizesAndReturnCode >>> 56) & 0xff;
        visitor.visitLabel(endTry);
        cleanup.forEach(i -> i.accept(visitor));
        visitor.visitInsn(returnCode);
        visitor.visitLabel(startCatch);
        visitor.visitFrame(F_SAME1, 0, null, 1, new Object[] { "java/lang/Throwable" });
        cleanup.forEach(i -> i.accept(visitor));
        visitor.visitInsn(ATHROW);
        visitor.visitLabel(endMethod);
        if (isInstanceMethod)
            visitor.visitLocalVariable("this", "L" + calleeClassName + ';', null, startMethod, endMethod, 0);
        visitor.visitMaxs((int)(sizesAndReturnCode >>> 28) & 0xfffffff, (int)(sizesAndReturnCode & 0xfffffff));
        visitor.visitEnd();
    }

    public static AnnotationVisitor checkForSimulationAnnotations(int api, String descriptor, AnnotationVisitor wrap, BiConsumer<Flag, Boolean> annotations)
    {
        if (!descriptor.equals("Lorg/apache/cassandra/utils/Simulate;"))
            return wrap;

        return new AnnotationVisitor(api, wrap)
        {
            @Override
            public AnnotationVisitor visitArray(String name)
            {
                if (!name.equals("with") && !name.equals("without"))
                    return super.visitArray(name);

                boolean add = name.equals("with");
                return new AnnotationVisitor(api, super.visitArray(name))
                {
                    @Override
                    public void visitEnum(String name, String descriptor, String value)
                    {
                        super.visitEnum(name, descriptor, value);
                        if (descriptor.equals("Lorg/apache/cassandra/utils/Simulate$With;"))
                            annotations.accept(Flag.valueOf(value), add);
                    }
                };
            }
        };
    }

    public static MethodVisitor deterministicToString(MethodVisitor wrap)
    {
        return new MethodVisitor(BYTECODE_VERSION, wrap)
        {
            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
            {
                if (name.equals("hashCode") && owner.equals("java/lang/Object"))
                {
                    super.visitMethodInsn(INVOKESTATIC, "java/lang/System", "identityHashCode", "(Ljava/lang/Object;)I", false);
                }
                else
                {
                    super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                }
            }
        };
    }

    public static void visitEachRefType(String descriptor, Consumer<String> forEach)
    {
        Type[] argTypes = Type.getArgumentTypes(descriptor);
        Type retType = Type.getReturnType(descriptor);
        for (Type argType : argTypes)
            visitIfRefType(argType.getDescriptor(), forEach);
        visitIfRefType(retType.getDescriptor(), forEach);
    }

    public static void visitIfRefType(String descriptor, Consumer<String> forEach)
    {
        if (descriptor.charAt(0) != '[' && descriptor.charAt(descriptor.length() - 1) != ';')
        {
            if (descriptor.length() > 1)
                forEach.accept(descriptor);
        }
        else
        {
            int i = 1;
            while (descriptor.charAt(i) == '[') ++i;
            if (descriptor.charAt(i) == 'L')
                forEach.accept(descriptor.substring(i + 1, descriptor.length() - 1));
        }
    }

    public static String descriptorToClassName(String desc)
    {
        // samples: "Ljdk/internal/misc/Unsafe;", "Lsun/misc/Unsafe;"
        if (!(desc.startsWith("L") && desc.endsWith(";")))
            throw new IllegalArgumentException("Unable to parse descriptor: " + desc);
        return desc.substring(1, desc.length() - 1);
    }
}

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

import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.simulator.asm.Flag.DETERMINISTIC;
import static org.apache.cassandra.simulator.asm.Flag.GLOBAL_METHODS;
import static org.apache.cassandra.simulator.asm.Flag.MONITORS;
import static org.apache.cassandra.simulator.asm.Flag.NEMESIS;
import static org.apache.cassandra.simulator.asm.Flag.NO_PROXY_METHODS;
import static org.apache.cassandra.simulator.asm.TransformationKind.HASHCODE;
import static org.apache.cassandra.simulator.asm.TransformationKind.SYNCHRONIZED;
import static org.apache.cassandra.simulator.asm.Utils.deterministicToString;
import static org.apache.cassandra.simulator.asm.Utils.visitEachRefType;
import static org.apache.cassandra.simulator.asm.Utils.generateTryFinallyProxyCall;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

class ClassTransformer extends ClassVisitor implements MethodWriterSink
{
    private static final List<AbstractInsnNode> DETERMINISM_SETUP = singletonList(new MethodInsnNode(INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptibleThread", "enterDeterministicMethod", "()V", false));
    private static final List<AbstractInsnNode> DETERMINISM_CLEANUP = singletonList(new MethodInsnNode(INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptibleThread", "exitDeterministicMethod", "()V", false));

    class DependentTypeVisitor extends MethodVisitor
    {
        public DependentTypeVisitor(int api, MethodVisitor methodVisitor)
        {
            super(api, methodVisitor);
        }

        @Override
        public void visitTypeInsn(int opcode, String type)
        {
            super.visitTypeInsn(opcode, type);
            Utils.visitIfRefType(type, dependentTypes);
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String descriptor)
        {
            super.visitFieldInsn(opcode, owner, name, descriptor);
            Utils.visitIfRefType(descriptor, dependentTypes);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
        {
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            Utils.visitEachRefType(descriptor, dependentTypes);
        }

        @Override
        public void visitInvokeDynamicInsn(String name, String descriptor, Handle bootstrapMethodHandle, Object... bootstrapMethodArguments)
        {
            super.visitInvokeDynamicInsn(name, descriptor, bootstrapMethodHandle, bootstrapMethodArguments);
            Utils.visitEachRefType(descriptor, dependentTypes);
        }

        @Override
        public void visitLocalVariable(String name, String descriptor, String signature, Label start, Label end, int index)
        {
            super.visitLocalVariable(name, descriptor, signature, start, end, index);
            Utils.visitIfRefType(descriptor, dependentTypes);
        }
    }

    private final String className;
    private final ChanceSupplier monitorDelayChance;
    private final NemesisGenerator nemesis;
    private final NemesisFieldKind.Selector nemesisFieldSelector;
    private final Hashcode insertHashcode;
    private final MethodLogger methodLogger;
    private boolean isTransformed;
    private boolean isCacheablyTransformed = true;
    private final EnumSet<Flag> flags;
    private final Consumer<String> dependentTypes;

    private boolean updateVisibility = false;

    ClassTransformer(int api, String className, EnumSet<Flag> flags, Consumer<String> dependentTypes)
    {
        this(api, new ClassWriter(0), className, flags, null, null, null, null, dependentTypes);
    }

    ClassTransformer(int api, String className, EnumSet<Flag> flags, ChanceSupplier monitorDelayChance, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode, Consumer<String> dependentTypes)
    {
        this(api, new ClassWriter(0), className, flags, monitorDelayChance, nemesis, nemesisFieldSelector, insertHashcode, dependentTypes);
    }

    private ClassTransformer(int api, ClassWriter classWriter, String className, EnumSet<Flag> flags, ChanceSupplier monitorDelayChance, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode, Consumer<String> dependentTypes)
    {
        super(api, classWriter);
        if (flags.contains(NEMESIS) && (nemesis == null || nemesisFieldSelector == null))
            throw new IllegalArgumentException();
        if (flags.contains(MONITORS) && monitorDelayChance == null)
            throw new IllegalArgumentException();
        this.dependentTypes = dependentTypes;
        this.className = className;
        this.flags = flags;
        this.monitorDelayChance = monitorDelayChance;
        this.nemesis = nemesis;
        this.nemesisFieldSelector = nemesisFieldSelector;
        this.insertHashcode = insertHashcode;
        this.methodLogger = MethodLogger.log(api, className);
    }

    public void setUpdateVisibility(boolean updateVisibility)
    {
        this.updateVisibility = updateVisibility;
    }

    /**
     * Java 11 changed the way that classes defined in the same source file get access to private state (see https://openjdk.org/jeps/181),
     * rather than trying to adapt to this, this method attempts to make the field/method/class public so that access
     * is not restricted.
     */
    private int makePublic(int access)
    {
        if (!updateVisibility)
            return access;
        // leave non-user created methods/fields/etc. alone
        if (contains(access, Opcodes.ACC_BRIDGE) || contains(access, Opcodes.ACC_SYNTHETIC))
            return access;
        if (contains(access, Opcodes.ACC_PRIVATE))
        {
            access &= ~Opcodes.ACC_PRIVATE;
            access |= Opcodes.ACC_PUBLIC;
        }
        else if (contains(access, Opcodes.ACC_PROTECTED))
        {
            access &= ~Opcodes.ACC_PROTECTED;
            access |= Opcodes.ACC_PUBLIC;
        }
        else if (!contains(access, Opcodes.ACC_PUBLIC)) // package-protected
        {
            access |= Opcodes.ACC_PUBLIC;
        }
        return access;
    }

    private static boolean contains(int value, int mask)
    {
        return (value & mask) != 0;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
    {
        super.visit(version, makePublic(access), name, signature, superName, interfaces);

    }

    @Override
    public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value)
    {
        if (dependentTypes != null)
            Utils.visitIfRefType(descriptor, dependentTypes);
        return super.visitField(makePublic(access), name, descriptor, signature, value);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
    {
        if (dependentTypes != null)
            visitEachRefType(descriptor, dependentTypes);

        EnumSet<Flag> flags = this.flags;
        if (flags.isEmpty() || ((access & ACC_SYNTHETIC) != 0 && (name.endsWith("$unsync") || name.endsWith("$catch") || name.endsWith("$nemesis"))))
        {
            MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
            if (dependentTypes != null && (access & (ACC_STATIC | ACC_SYNTHETIC)) != 0 && (name.equals("<clinit>") || name.startsWith("lambda$")))
                visitor = new DependentTypeVisitor(api, visitor);
            return visitor;
        }

        boolean isToString = false;
        if (access == Opcodes.ACC_PUBLIC && name.equals("toString") && descriptor.equals("()Ljava/lang/String;") && !flags.contains(NO_PROXY_METHODS))
        {
            generateTryFinallyProxyCall(super.visitMethod(access, name, descriptor, signature, exceptions), className,
                                        "toString$original", "()Ljava/lang/String;", access, true, false, DETERMINISM_SETUP, DETERMINISM_CLEANUP);
            access = ACC_PRIVATE | ACC_SYNTHETIC;
            name = "toString$original";
            if (!flags.contains(DETERMINISTIC) || flags.contains(NEMESIS))
            {
                flags = EnumSet.copyOf(flags);
                flags.add(DETERMINISTIC);
                flags.remove(NEMESIS);
            }
            isToString = true;
        }

        access = makePublic(access);
        MethodVisitor visitor;
        if (flags.contains(MONITORS) && (access & Opcodes.ACC_SYNCHRONIZED) != 0)
        {
            visitor = new MonitorMethodTransformer(this, className, api, access, name, descriptor, signature, exceptions, monitorDelayChance);
            witness(SYNCHRONIZED);
        }
        else
        {
            visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
            visitor = methodLogger.visitMethod(access, name, descriptor, visitor);
        }

        if (flags.contains(MONITORS))
            visitor = new MonitorEnterExitParkTransformer(this, api, visitor, className, monitorDelayChance);
        if (isToString)
            visitor = deterministicToString(visitor);
        if (flags.contains(GLOBAL_METHODS) || flags.contains(Flag.LOCK_SUPPORT) || flags.contains(Flag.DETERMINISTIC))
            visitor = new GlobalMethodTransformer(flags, this, api, name, visitor);
        if (flags.contains(NEMESIS))
            visitor = new NemesisTransformer(this, api, name, visitor, nemesis, nemesisFieldSelector);
        if (dependentTypes != null && (access & (ACC_STATIC | ACC_SYNTHETIC)) != 0 && (name.equals("<clinit>") || name.startsWith("lambda$")))
            visitor = new DependentTypeVisitor(api, visitor);
        return visitor;
    }

    @Override
    public void visitEnd()
    {
        if (insertHashcode != null)
            writeSyntheticMethod(HASHCODE, insertHashcode);
        super.visitEnd();
        methodLogger.visitEndOfClass();
    }

    public void writeMethod(MethodNode node)
    {
        writeMethod(null, node);
    }

    public void writeSyntheticMethod(TransformationKind kind, MethodNode node)
    {
        writeMethod(kind, node);
    }

    void writeMethod(TransformationKind kind, MethodNode node)
    {
        String[] exceptions = node.exceptions == null ? null : node.exceptions.toArray(new String[0]);
        MethodVisitor visitor = super.visitMethod(node.access, node.name, node.desc, node.signature, exceptions);
        visitor = methodLogger.visitMethod(node.access, node.name, node.desc, visitor);
        if (kind != null)
            witness(kind);
        node.accept(visitor);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible)
    {
        return Utils.checkForSimulationAnnotations(api, descriptor, super.visitAnnotation(descriptor, visible), (flag, add) -> {
            if (add) flags.add(flag);
            else flags.remove(flag);
        });
    }

    void readAndTransform(byte[] input)
    {
        ClassReader reader = new ClassReader(input);
        reader.accept(this, 0);
    }

    void witness(TransformationKind kind)
    {
        isTransformed = true;
        switch (kind)
        {
            case FIELD_NEMESIS:
            case SIGNAL_NEMESIS:
                isCacheablyTransformed = false;
        }
        methodLogger.witness(kind);
    }

    String className()
    {
        return className;
    }

    boolean isTransformed()
    {
        return isTransformed;
    }

    boolean isCacheablyTransformed()
    {
        return isCacheablyTransformed;
    }

    byte[] toBytes()
    {
        return ((ClassWriter) cv).toByteArray();
    }
}

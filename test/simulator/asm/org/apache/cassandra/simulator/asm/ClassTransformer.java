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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
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
import static org.apache.cassandra.simulator.asm.Utils.generateTryFinallyProxyCall;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

class ClassTransformer extends ClassVisitor implements MethodWriterSink
{
    private static final List<AbstractInsnNode> DETERMINISM_SETUP = singletonList(new MethodInsnNode(INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptibleThread", "enterDeterministicMethod", "()V", false));
    private static final List<AbstractInsnNode> DETERMINISM_CLEANUP = singletonList(new MethodInsnNode(INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptibleThread", "exitDeterministicMethod", "()V", false));

    private final String className;
    private final ChanceSupplier monitorDelayChance;
    private final NemesisGenerator nemesis;
    private final NemesisFieldKind.Selector nemesisFieldSelector;
    private final Hashcode insertHashcode;
    private final MethodLogger methodLogger;
    private boolean isTransformed;
    private boolean isCacheablyTransformed = true;
    private final EnumSet<Flag> flags;

    ClassTransformer(int api, String className, EnumSet<Flag> flags)
    {
        this(api, new ClassWriter(0), className, flags, null, null, null, null);
    }

    ClassTransformer(int api, String className, EnumSet<Flag> flags, ChanceSupplier monitorDelayChance, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode)
    {
        this(api, new ClassWriter(0), className, flags, monitorDelayChance, nemesis, nemesisFieldSelector, insertHashcode);
    }

    private ClassTransformer(int api, ClassWriter classWriter, String className, EnumSet<Flag> flags, ChanceSupplier monitorDelayChance, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode)
    {
        super(api, classWriter);
        if (flags.contains(NEMESIS) && (nemesis == null || nemesisFieldSelector == null))
            throw new IllegalArgumentException();
        if (flags.contains(MONITORS) && monitorDelayChance == null)
            throw new IllegalArgumentException();
        this.className = className;
        this.flags = flags;
        this.monitorDelayChance = monitorDelayChance;
        this.nemesis = nemesis;
        this.nemesisFieldSelector = nemesisFieldSelector;
        this.insertHashcode = insertHashcode;
        this.methodLogger = MethodLogger.log(api, className);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
    {
        EnumSet<Flag> flags = this.flags;
        if (flags.isEmpty() || ((access & ACC_SYNTHETIC) != 0 && (name.endsWith("$unsync") || name.endsWith("$catch") || name.endsWith("$nemesis"))))
            return super.visitMethod(access, name, descriptor, signature, exceptions);

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

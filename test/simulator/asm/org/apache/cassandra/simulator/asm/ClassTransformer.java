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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.MethodNode;

import static org.apache.cassandra.simulator.asm.TransformationKind.HASHCODE;

class ClassTransformer extends ClassVisitor
{
    private final String className;
    private final ChanceSupplier monitorDelayChance;
    private final boolean factories;
    private final NemesisGenerator nemesis;
    private final NemesisFieldKind.Selector nemesisFieldSelector;
    private final Hashcode insertHashcode;
    private final MethodLogger methodLogger;
    private boolean isTransformed;
    private boolean isCacheablyTransformed = true;

    ClassTransformer(int api, String className, ChanceSupplier monitorDelayChance, boolean factories, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode)
    {
        this(api, new ClassWriter(0), className, monitorDelayChance, factories, nemesis, nemesisFieldSelector, insertHashcode);
    }

    private ClassTransformer(int api, ClassWriter classWriter, String className, ChanceSupplier monitorDelayChance, boolean factories, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode)
    {
        super(api, classWriter);
        this.className = className;
        this.monitorDelayChance = monitorDelayChance;
        this.factories = factories;
        this.nemesis = nemesis;
        this.nemesisFieldSelector = nemesisFieldSelector;
        this.insertHashcode = insertHashcode;
        this.methodLogger = MethodLogger.log(api, className);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
    {
        super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
    {
        if (!(factories || monitorDelayChance != null || nemesis != null) || ((access & Opcodes.ACC_SYNTHETIC) != 0 && (name.endsWith("$original") || name.endsWith("$catch") || name.endsWith("$nemesis"))))
            return super.visitMethod(access, name, descriptor, signature, exceptions);

        MethodVisitor visitor;
        if (monitorDelayChance != null && (access & Opcodes.ACC_SYNCHRONIZED) != 0)
        {
            visitor = new MonitorMethodTransformer(this, className, api, access, name, descriptor, signature, exceptions, monitorDelayChance);
        }
        else
        {
            visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
            visitor = methodLogger.visitMethod(access, name, descriptor, visitor);
        }

        if (monitorDelayChance != null) visitor = new MonitorEnterExitParkTransformer(this, api, visitor, className, monitorDelayChance);
        if (factories) visitor = new GlobalMethodTransformer(this, api, visitor);
        if (nemesis != null) visitor = new NemesisTransformer(this, api, name, visitor, nemesis, nemesisFieldSelector);
        return visitor;
    }

    @Override
    public void visitEnd()
    {
        if (insertHashcode != null)
            write(HASHCODE, insertHashcode);
        super.visitEnd();
        methodLogger.visitEndOfClass();
    }

    void write(TransformationKind kind, MethodNode node)
    {
        String[] exceptions = node.exceptions == null ? null : node.exceptions.toArray(new String[0]);
        MethodVisitor visitor = super.visitMethod(node.access, node.name, node.desc, node.signature, exceptions);
        visitor = methodLogger.visitMethod(node.access, node.name, node.desc, visitor);
        if (kind != null)
            witness(kind);
        node.accept(visitor);
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

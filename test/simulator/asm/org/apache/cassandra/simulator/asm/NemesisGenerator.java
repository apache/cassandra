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

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

/**
 * Generate a new static method in the class with a randomly generated constant chance of triggering the nemesis.
 * Generate also the invocation of this method at the relevant point(s).
 *
 * A static method with no parameters or return values is created, so that only the method invocation instruction is
 * needed in the original method, simplifying the transformation.
 */
class NemesisGenerator extends MethodNode
{
    private final ChanceSupplier chanceSupplier;

    private final String className;
    private String baseName;
    private int methodsCounter = 0; // avoid nemesis method name clashes when weaving two or more methods with same name
    private int withinMethodCounter = 0;
    private final LdcInsnNode ldc = new LdcInsnNode(null);

    NemesisGenerator(int api, String className, ChanceSupplier chanceSupplier)
    {
        super(api, Opcodes.ACC_STATIC | Opcodes.ACC_SYNTHETIC | Opcodes.ACC_PRIVATE, null, "()V", "", null);
        this.chanceSupplier = chanceSupplier;
        this.className = className;
        this.maxLocals = 0;
        this.maxStack = 1;
        instructions.add(new LabelNode());
        instructions.add(ldc);
        instructions.add(new MethodInsnNode(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "nemesis", "(F)V", false));
        instructions.add(new LabelNode());
        instructions.add(new InsnNode(Opcodes.RETURN));
    }

    void newMethod(String name)
    {
        this.baseName = name.replaceAll("[<>]", "") + '$' + (methodsCounter++) + '$';
        this.withinMethodCounter = 0;
    }

    void generateAndCall(TransformationKind kind, ClassTransformer writeDefinitionTo, MethodVisitor writeInvocationTo)
    {
        this.name = baseName + '$' + (withinMethodCounter++) + "$nemesis";
        ldc.cst = chanceSupplier.get();
        writeDefinitionTo.writeSyntheticMethod(kind, this);
        writeInvocationTo.visitMethodInsn(Opcodes.INVOKESTATIC, className, name, "()V", false);
    }
}

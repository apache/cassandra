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

import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import static org.apache.cassandra.simulator.asm.TransformationKind.FIELD_NEMESIS;
import static org.apache.cassandra.simulator.asm.TransformationKind.SIGNAL_NEMESIS;

/**
 * Insert nemesis points at all obvious thread signalling points (execution and blocking primitive methods),
 * as well as to any fields annotated with {@link org.apache.cassandra.utils.Nemesis}.
 *
 * If the annotated field is an AtomicX or AtomicXFieldUpdater, we insert nemesis points either side of the next
 * invocation of
 *
 * TODO (config): permit Nemesis on a class as well as a field, so as to mark all (at least volatile or atomic) members
 */
class NemesisTransformer extends MethodVisitor
{
    private final ClassTransformer transformer;
    final NemesisGenerator generator;
    final NemesisFieldKind.Selector nemesisFieldSelector;

    // for simplicity, we simply activate nemesis for all atomic operations on the relevant type once any such
    // field is loaded in a method
    Set<String> onForTypes;

    public NemesisTransformer(ClassTransformer transformer, int api, String name, MethodVisitor parent, NemesisGenerator generator, NemesisFieldKind.Selector nemesisFieldSelector)
    {
        super(api, parent);
        this.transformer = transformer;
        this.generator = generator;
        this.nemesisFieldSelector = nemesisFieldSelector;
        generator.newMethod(name);
    }

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
    {
        boolean nemesisAfter = false;
        if (isInterface && opcode == Opcodes.INVOKEINTERFACE
        && (owner.startsWith("org/apache/cassandra/concurrent") || owner.startsWith("org/apache/cassandra/utils/concurrent")) && (
               (owner.equals("org/apache/cassandra/utils/concurrent/CountDownLatch") && name.equals("decrement"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Condition") && name.equals("signal"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Semaphore") && name.equals("release"))
            || ((owner.equals("org/apache/cassandra/concurrent/ExecutorPlus")
                 || owner.equals("org/apache/cassandra/concurrent/LocalAwareExecutorPlus")
                 || owner.equals("org/apache/cassandra/concurrent/ScheduledExecutorPlus")
                 || owner.equals("org/apache/cassandra/concurrent/SequentialExecutorPlus")
                 || owner.equals("org/apache/cassandra/concurrent/LocalAwareSequentialExecutorPlus")
                ) && (name.equals("execute") || name.equals("submit") || name.equals("maybeExecuteImmediately")))
        ))
        {
            generateAndCall(SIGNAL_NEMESIS);
        }
        else if ((opcode == Opcodes.INVOKESPECIAL || opcode == Opcodes.INVOKEVIRTUAL)
                 && (onForTypes != null && onForTypes.contains(owner)))
        {
            nemesisAfter = true;
            generateAndCall(FIELD_NEMESIS);
        }

        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
        if (nemesisAfter)
            generateAndCall(FIELD_NEMESIS);
    }

    @Override
    public void visitFieldInsn(int opcode, String owner, String name, String descriptor)
    {
        boolean nemesisAfter = false;
        NemesisFieldKind nemesis = nemesisFieldSelector.get(owner, name);
        if (nemesis != null)
        {
            switch (nemesis)
            {
                case SIMPLE:
                    switch (opcode)
                    {
                        default:
                            throw new AssertionError();
                        case Opcodes.PUTFIELD:
                        case Opcodes.PUTSTATIC:
                            generateAndCall(FIELD_NEMESIS);
                            break;
                        case Opcodes.GETFIELD:
                        case Opcodes.GETSTATIC:
                            nemesisAfter = true;
                    }
                    break;
                case ATOMICX:
                case ATOMICUPDATERX:
                    switch (opcode)
                    {
                        case Opcodes.GETFIELD:
                        case Opcodes.GETSTATIC:
                            if (onForTypes == null)
                                onForTypes = new HashSet<>();
                            onForTypes.add(descriptor.substring(1, descriptor.length() - 1));
                    }
                    break;
            }
        }
        super.visitFieldInsn(opcode, owner, name, descriptor);
        if (nemesisAfter)
            generateAndCall(FIELD_NEMESIS);
    }

    private void generateAndCall(TransformationKind kind)
    {
        generator.generateAndCall(kind, transformer, mv);
    }
}

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

import static org.apache.cassandra.simulator.asm.TransformationKind.LOCK_SUPPORT;
import static org.apache.cassandra.simulator.asm.TransformationKind.MONITOR;

/**
 * Handle simply thread signalling behaviours, namely monitorenter/monitorexit bytecodes to
 * {@link org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods}, and LockSupport invocations to
 * {@link org.apache.cassandra.simulator.systems.InterceptibleThread}.
 *
 * The global static methods we redirect monitors to take only one parameter (the monitor) and also return it,
 * so that they have net zero effect on the stack, permitting the existing monitorenter/monitorexit instructions
 * to remain where they are. LockSupport on the other hand is redirected entirely to the new method.
 */
class MonitorEnterExitParkTransformer extends MethodVisitor
{
    private final ClassTransformer transformer;
    private final String className;
    private final ChanceSupplier monitorDelayChance;

    public MonitorEnterExitParkTransformer(ClassTransformer transformer,
                                           int api,
                                           MethodVisitor parent,
                                           String className,
                                           ChanceSupplier monitorDelayChance)
    {
        super(api, parent);
        this.transformer = transformer;
        this.className = className;
        this.monitorDelayChance = monitorDelayChance;
    }

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
    {
        if (opcode == Opcodes.INVOKEVIRTUAL && !isInterface && owner.equals("java/lang/Object"))
        {
            switch (name.charAt(0))
            {
                case 'w':
                    assert name.equals("wait");
                    switch (descriptor.charAt(2))
                    {
                        default:
                            throw new AssertionError("Unexpected descriptor for method wait() in " + className + '.' + name);
                        case 'V': // ()V
                            transformer.witness(MONITOR);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "wait", "(Ljava/lang/Object;)V", false);
                            return;
                        case ')': // (J)V
                            transformer.witness(MONITOR);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "wait", "(Ljava/lang/Object;J)V", false);
                            return;
                        case 'I': // (JI)V
                            transformer.witness(MONITOR);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "wait", "(Ljava/lang/Object;JI)V", false);
                            return;
                    }
                case 'n':
                    switch (name.length())
                    {
                        default:
                            throw new AssertionError();
                        case 6: // notify
                            transformer.witness(MONITOR);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "notify", "(Ljava/lang/Object;)V", false);
                            return;

                        case 9: // notifyAll
                            transformer.witness(MONITOR);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "notifyAll", "(Ljava/lang/Object;)V", false);
                            return;
                    }
            }
        }
        if (opcode == Opcodes.INVOKESTATIC && !isInterface && owner.equals("java/util/concurrent/locks/LockSupport"))
        {
            transformer.witness(LOCK_SUPPORT);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptibleThread", name, descriptor, false);
            return;
        }
        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
    }

    @Override
    public void visitInsn(int opcode)
    {
        switch (opcode)
        {
            case Opcodes.MONITORENTER:
                transformer.witness(MONITOR);
                super.visitLdcInsn(monitorDelayChance.get());
                super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "preMonitorEnter", "(Ljava/lang/Object;F)Ljava/lang/Object;", false);
                break;
            case Opcodes.MONITOREXIT:
                transformer.witness(MONITOR);
                super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "preMonitorExit", "(Ljava/lang/Object;)Ljava/lang/Object;", false);
                break;
        }
        super.visitInsn(opcode);
    }
}

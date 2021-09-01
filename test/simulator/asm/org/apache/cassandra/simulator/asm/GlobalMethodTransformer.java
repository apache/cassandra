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

import static org.apache.cassandra.simulator.asm.TransformationKind.GLOBAL_METHOD;
import static org.apache.cassandra.simulator.asm.TransformationKind.IDENTITY_HASH_MAP;

/**
 * Intercept factory methods in org.apache.concurrent.utils.concurrent, and redirect them to
 * {@link org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods}
 */
class GlobalMethodTransformer extends MethodVisitor
{
    private final ClassTransformer transformer;

    public GlobalMethodTransformer(ClassTransformer transformer, int api, MethodVisitor parent)
    {
        super(api, parent);
        this.transformer = transformer;
    }

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
    {
        if (opcode == Opcodes.INVOKESTATIC && (owner.startsWith("org/apache/cassandra/utils/") && (
               (owner.equals("org/apache/cassandra/utils/concurrent/WaitQueue") && name.equals("newWaitQueue"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/CountDownLatch") && name.equals("newCountDownLatch"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Condition") && name.equals("newOneTimeCondition"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/BlockingQueues") && name.equals("newBlockingQueue"))
            || (owner.equals("org/apache/cassandra/utils/Clock") && name.equals("waitUntil"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Awaitable$SyncAwaitable") && name.equals("waitUntil"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Semaphore") && (name.equals("newSemaphore") || name.equals("newFairSemaphore")))
            )) || (owner.equals("java/lang/System") && name.equals("identityHashCode")))
        {
            transformer.witness(GLOBAL_METHOD);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfGlobalMethods$Global", name, descriptor, false);
            return;
        }
        else if (opcode == Opcodes.INVOKESPECIAL && (owner.equals("java/util/IdentityHashMap") && name.equals("<init>")))
        {
            transformer.witness(IDENTITY_HASH_MAP);
            super.visitMethodInsn(opcode, "org/apache/cassandra/simulator/systems/InterceptedIdentityHashMap", name, descriptor, false);
            return;
        }

        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
    }

    @Override
    public void visitTypeInsn(int opcode, String type)
    {
        if (opcode == Opcodes.NEW && type.equals("java/util/IdentityHashMap"))
        {
            super.visitTypeInsn(opcode, "org/apache/cassandra/simulator/systems/InterceptedIdentityHashMap");
            return;
        }
        super.visitTypeInsn(opcode, type);
    }
}

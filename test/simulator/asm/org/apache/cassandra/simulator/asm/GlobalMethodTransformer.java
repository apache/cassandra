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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import static org.apache.cassandra.simulator.asm.Flag.GLOBAL_METHODS;
import static org.apache.cassandra.simulator.asm.TransformationKind.CONCURRENT_HASH_MAP;
import static org.apache.cassandra.simulator.asm.TransformationKind.GLOBAL_METHOD;
import static org.apache.cassandra.simulator.asm.TransformationKind.IDENTITY_HASH_MAP;

/**
 * Intercept factory methods in org.apache.concurrent.utils.concurrent, and redirect them to
 * {@link org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods}
 */
class GlobalMethodTransformer extends MethodVisitor
{
    private final ClassTransformer transformer;
    private final String methodName;
    private boolean globalMethods;
    private boolean globalClock;
    private boolean systemClock;
    private boolean lockSupport;
    private boolean deterministic;
    boolean hasSeenAnyMethodInsn;

    public GlobalMethodTransformer(EnumSet<Flag> flags, ClassTransformer transformer, int api, String methodName, MethodVisitor parent)
    {
        super(api, parent);
        this.globalMethods = flags.contains(GLOBAL_METHODS);
        this.globalClock = flags.contains(Flag.GLOBAL_CLOCK);
        this.systemClock = flags.contains(Flag.SYSTEM_CLOCK);
        this.lockSupport = flags.contains(Flag.LOCK_SUPPORT);
        this.deterministic = flags.contains(Flag.DETERMINISTIC);
        this.transformer = transformer;
        this.methodName = methodName;
    }

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
    {
        boolean isFirstMethodInsn = !hasSeenAnyMethodInsn;
        hasSeenAnyMethodInsn = true;

        if (globalMethods && opcode == Opcodes.INVOKESTATIC && owner.startsWith("org/apache/cassandra/utils/") && (
               (owner.equals("org/apache/cassandra/utils/concurrent/WaitQueue") && name.equals("newWaitQueue"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/CountDownLatch") && name.equals("newCountDownLatch"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Condition") && name.equals("newOneTimeCondition"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/BlockingQueues") && name.equals("newBlockingQueue"))
            || (owner.equals("org/apache/cassandra/utils/concurrent/Semaphore") && (name.equals("newSemaphore") || name.equals("newFairSemaphore")))
            ))
        {
            transformer.witness(GLOBAL_METHOD);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfGlobalMethods$Global", name, descriptor, false);
        }
        else if (globalMethods && ((opcode == Opcodes.INVOKESTATIC && (
                   owner.startsWith("org/apache/cassandra/utils/") && (
                        (owner.equals("org/apache/cassandra/utils/Clock") && name.equals("waitUntil"))
                     || (owner.equals("org/apache/cassandra/utils/concurrent/Awaitable$SyncAwaitable") && name.equals("waitUntil")))
                || !deterministic && owner.equals("java/lang/System") && name.equals("identityHashCode")
                || owner.equals("java/util/UUID") && name.equals("randomUUID")
                || owner.equals("com/google/common/util/concurrent/Uninterruptibles") && name.equals("sleepUninterruptibly")
                || owner.equals("sun/misc/Unsafe") && name.equals("getUnsafe")))
             || (owner.equals("java/util/concurrent/TimeUnit") && name.equals("sleep")))
        )
        {
            transformer.witness(GLOBAL_METHOD);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", name, descriptor, false);
        }
        else if ((globalMethods || deterministic) && opcode == Opcodes.INVOKESTATIC &&
            owner.equals("java/util/concurrent/ThreadLocalRandom") && (name.equals("getProbe") || name.equals("advanceProbe") || name.equals("localInit"))
        )
        {
            transformer.witness(GLOBAL_METHOD);
            // if we're in deterministic mode (i.e. for base ConcurrentHashMap) don't initialise ThreadLocalRandom
            if (name.equals("getProbe")) super.visitLdcInsn(0);
            else if (name.equals("advanceProbe")) super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", name, descriptor, false);
        }
        else if (globalMethods && opcode == Opcodes.INVOKESPECIAL && owner.equals("java/util/IdentityHashMap") && name.equals("<init>"))
        {
            transformer.witness(IDENTITY_HASH_MAP);
            super.visitMethodInsn(opcode, "org/apache/cassandra/simulator/systems/InterceptedIdentityHashMap", name, descriptor, false);
        }
        else if (globalMethods && opcode == Opcodes.INVOKESPECIAL && owner.equals("java/util/concurrent/ConcurrentHashMap") && name.equals("<init>")
                 && !(transformer.className().equals("org/apache/cassandra/simulator/systems/InterceptibleConcurrentHashMap") && methodName.equals("<init>") && isFirstMethodInsn))
        {
            transformer.witness(CONCURRENT_HASH_MAP);
            super.visitMethodInsn(opcode, "org/apache/cassandra/simulator/systems/InterceptibleConcurrentHashMap", name, descriptor, false);
        }
        else if (lockSupport && opcode == Opcodes.INVOKESTATIC && owner.equals("java/util/concurrent/locks/LockSupport") && (name.startsWith("park") || name.equals("unpark")))
        {
            transformer.witness(TransformationKind.LOCK_SUPPORT);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", name, descriptor, false);
        }
        else if (globalClock && opcode == Opcodes.INVOKESTATIC && name.equals("timestampMicros") && owner.equals("org/apache/cassandra/utils/FBUtilities"))
        {
            transformer.witness(GLOBAL_METHOD);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/SimulatedTime$Global", "nextGlobalMonotonicMicros", descriptor, false);
        }
        else if (systemClock && opcode == Opcodes.INVOKESTATIC && owner.equals("java/lang/System") && (name.equals("nanoTime") || name.equals("currentTimeMillis")))
        {
            transformer.witness(GLOBAL_METHOD);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", name, descriptor, false);
        }
        else
        {
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
        }
    }

    @Override
    public void visitTypeInsn(int opcode, String type)
    {
        if (globalMethods && opcode == Opcodes.NEW && type.equals("java/util/IdentityHashMap"))
        {
            super.visitTypeInsn(opcode, "org/apache/cassandra/simulator/systems/InterceptedIdentityHashMap");
        }
        else if (globalMethods && opcode == Opcodes.NEW && type.equals("java/util/concurrent/ConcurrentHashMap"))
        {
            super.visitTypeInsn(opcode, "org/apache/cassandra/simulator/systems/InterceptibleConcurrentHashMap");
        }
        else
        {
            super.visitTypeInsn(opcode, type);
        }
    }

    @Override
    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible)
    {
        return Utils.checkForSimulationAnnotations(api, descriptor, super.visitAnnotation(descriptor, visible), (flag, add) -> {
            switch (flag)
            {
                default: throw new AssertionError();
                case GLOBAL_METHODS: globalMethods = add; break;
                case GLOBAL_CLOCK: globalClock = add; break;
                case SYSTEM_CLOCK: systemClock = add; break;
                case LOCK_SUPPORT: lockSupport = add; break;
                case DETERMINISTIC: deterministic = add; break;
                case MONITORS: throw new UnsupportedOperationException("Cannot currently toggle MONITORS at the method level");
            }
        });
    }
}

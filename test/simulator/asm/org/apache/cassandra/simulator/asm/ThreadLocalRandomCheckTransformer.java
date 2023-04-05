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

/**
 * Handle simply thread signalling behaviours, namely monitorenter/monitorexit bytecodes to
 * {@link org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods}, and LockSupport invocations to
 * {@link org.apache.cassandra.simulator.systems.InterceptibleThread}.
 *
 * The global static methods we redirect monitors to take only one parameter (the monitor) and also return it,
 * so that they have net zero effect on the stack, permitting the existing monitorenter/monitorexit instructions
 * to remain where they are. LockSupport on the other hand is redirected entirely to the new method.
 */
class ThreadLocalRandomCheckTransformer extends MethodVisitor
{
    public ThreadLocalRandomCheckTransformer(int api, MethodVisitor parent)
    {
        super(api, parent);
    }

    @Override
    public void visitInsn(int opcode)
    {
        switch (opcode)
        {
            case Opcodes.IRETURN:
                super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global",
                                      "threadLocalRandomCheck", "(I)I", false);
                break;
            case Opcodes.LRETURN:
                super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global",
                                      "threadLocalRandomCheck", "(J)J", false);
                break;
        }
        super.visitInsn(opcode);
    }
}

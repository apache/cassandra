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

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

/**
 * Generate a new hashCode method in the class that invokes a deterministic hashCode generator
 */
class Hashcode extends MethodNode
{
    Hashcode(int api)
    {
        super(api, Opcodes.ACC_PUBLIC, "hashCode", "()I", null, null);
        maxLocals = 1;
        maxStack = 1;
        instructions.add(new LabelNode());
        instructions.add(new IntInsnNode(Opcodes.ALOAD, 0));
        instructions.add(new MethodInsnNode(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "identityHashCode", "(Ljava/lang/Object;)I", false));
        instructions.add(new LabelNode());
        instructions.add(new InsnNode(Opcodes.IRETURN));
    }
}

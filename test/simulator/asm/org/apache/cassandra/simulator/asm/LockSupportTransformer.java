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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.MethodNode;

public class LockSupportTransformer
{

    static byte[] transform(byte[] originalLockSupport)
    {
        byte[] interceptedLockSupport;
        try (InputStream template = ClassLoader.getSystemClassLoader().getResourceAsStream("org/apache/cassandra/simulator/systems/InterceptingLockSupport.class"))
        {
            byte[] bytes = new byte[1024];
            int count = 0, read;
            while ((read = template.read(bytes, count, bytes.length - count)) >= 0)
            {
                if ((count += read) == bytes.length)
                    bytes = Arrays.copyOf(bytes, bytes.length * 2);
            }
            interceptedLockSupport = Arrays.copyOf(bytes, count);
        }
        catch (Throwable t)
        {
            System.err.println("Could not get template to weave LockSupport");
            t.printStackTrace();
            return originalLockSupport;
        }

        try
        {

            Set<String> transform = new HashSet<>(Arrays.asList("park", "parkUntil", "parkNanos", "unpark"));

            Map<String, MethodNode> nameAndDescriptorToIntercepts = new HashMap<>(); {
                ClassReader reader = new ClassReader(interceptedLockSupport);
                reader.accept(new ClassVisitor(InterceptClasses.BYTECODE_VERSION)
                {
                    @Override
                    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
                    {
                        if (transform.contains(name))
                        {
                            MethodNode saveTo = new MethodNode(access, name, descriptor, signature, exceptions);
                            nameAndDescriptorToIntercepts.put(name + descriptor, saveTo);
                            return saveTo;
                        }
                        return super.visitMethod(access, name, descriptor, signature, exceptions);
                    }
                }, 0);
            }

            ClassWriter writer = new ClassWriter(0);
            ClassReader reader = new ClassReader(originalLockSupport);
            reader.accept(new ClassVisitor(InterceptClasses.BYTECODE_VERSION, writer)
            {
                @Override
                public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
                {
                    if (transform.contains(name))
                    {
                        nameAndDescriptorToIntercepts.get(name + descriptor).accept(super.visitMethod(access, name, descriptor, signature, exceptions));
                        return super.visitMethod((access & ~Opcodes.ACC_PUBLIC) | Opcodes.ACC_PRIVATE, "real" + name.substring(0, 1).toUpperCase() + name.substring(1),
                                                 descriptor, signature, exceptions);
                    }
                    else
                    {
                        return super.visitMethod(access, name, descriptor, signature, exceptions);
                    }
                }
            }, 0);

            return writer.toByteArray();
        }
        catch (Throwable t)
        {
            System.err.println("Failed to transform LockSupport");
            t.printStackTrace();
            return originalLockSupport;
        }
    }

}

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

import java.lang.instrument.Instrumentation;
import java.util.Arrays;

// for classes loaded by bootstrap classloader
public class InterceptAgent
{
    private static boolean isLockSupportCaptured;

    public static boolean isLockSupportCaptured()
    {
        return isLockSupportCaptured;
    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        setup(agentArgs, instrumentation);
    }

    public void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        setup(agentArgs, instrumentation);
    }

    private static void setup(final String agentArgs, final Instrumentation instrumentation)
    {
        // TODO (now): intercept Enum and introduce hashCode() method to use ordinal() to ensure determinism
        String[] params = agentArgs == null ? new String[]{} : agentArgs.split(",");
        boolean withoutCHM = Arrays.stream(params).anyMatch("-concurrenthashmap"::equalsIgnoreCase);
        boolean withLockSupport = Arrays.stream(params).anyMatch("+locksupport"::equalsIgnoreCase);
        instrumentation.addTransformer((loader, className, classBeingRedefined, protectionDomain, classfileBuffer) -> {
            if (loader == null && !withoutCHM && className.startsWith("java/util/concurrent/ConcurrentHashMap"))
            {
                ClassTransformer transformer = new ClassTransformer(InterceptClasses.BYTECODE_VERSION, className, () -> 0f, false, null, null, null);
                transformer.readAndTransform(classfileBuffer);
                if (transformer.isTransformed())
                    return transformer.toBytes();
            }
            else if (withLockSupport && className.equals("java/util/concurrent/locks/LockSupport"))
            {
                byte[] transformed = LockSupportTransformer.transform(classfileBuffer);
                isLockSupportCaptured = transformed != classfileBuffer;
            }
            return classfileBuffer;
        });
    }
}

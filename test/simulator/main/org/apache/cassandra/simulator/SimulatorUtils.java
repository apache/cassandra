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

package org.apache.cassandra.simulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.utils.concurrent.Threads;

public class SimulatorUtils
{
    public static RuntimeException failWithOOM()
    {
        List<long[]> oom = new ArrayList<>();
        for (int i = 0 ; i < 1024 ; ++i)
            oom.add(new long[0x7fffffff]);
        throw new AssertionError();
    }

    public static void dumpStackTraces(Logger logger)
    {
        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        threadMap.forEach((thread, ste) -> {
            logger.error("{}:\n   {}", thread, Threads.prettyPrint(ste, false, "   ", "\n", ""));
        });
        FastThreadLocal.destroy();
    }
}

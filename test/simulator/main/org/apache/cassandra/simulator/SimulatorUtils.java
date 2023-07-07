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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        String prefix = "   ";
        String delimiter = "\n" + prefix;
        threadMap.forEach((thread, ste) ->
                          logger.error("{}:\n{}", thread, Threads.prettyPrint(ste, false, prefix, delimiter, "")));
        FastThreadLocal.destroy();
    }

    public static void verifyAndlogSimulatorArgs(Logger logger, String[] args)
    {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        final List<String> jvmArgs = runtimeMxBean.getInputArguments();
        logger.error("JVM Args: {}", jvmArgs.stream().collect(Collectors.joining("\" \"", "\"", "\"")));
        logger.error("Command Args: {}", Arrays.stream(args).collect(Collectors.joining("\" \"", "\"", "\"")));

        assert jvmArgs.stream().anyMatch(arg -> arg.startsWith("-Xbootclasspath/a") && arg.endsWith("simulator-bootstrap.jar")) :
        "must launch JVM with -Xbootclasspath/a:simulator-bootstrap.jar";
        assert jvmArgs.stream().anyMatch(arg -> arg.startsWith("-javaagent:") && arg.endsWith("simulator-asm.jar")) :
        "must launch JVM with -javaagent:simulator-asm.jar";
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-XX:-BackgroundCompilation")))
            logger.warn("JVM Argument -XX:-BackgroundCompilation not set, non-determinism possible");
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-XX:-TieredCompilation")))
            logger.warn("JVM Argument -XX:-TieredCompilation not set, non-determinism possible");
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-XX:CICompilerCount=1")))
            logger.warn("JVM Argument -XX:CICompilerCount=1 not set, non-determinism possible");
        if (!jvmArgs.stream().anyMatch(arg -> arg.startsWith("-XX:Tier4CompileThreshold=")))
            logger.warn("JVM Argument -XX:Tier4CompileThreshold not set, non-determinism possible. Typically set -XX:Tier4CompileThreshold=1000");
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-Dcassandra.disable_tcactive_openssl=true")))
            logger.warn("JVM Argument -Dcassandra.disable_tcactive_openssl=true not set, non-determinism possible. Typically set -XX:Tier4CompileThreshold=1000");

        // log4j support
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-Dlog4j2.disableJmx=true")))
            logger.warn("JVM Argument -Dlog4j2.disableJmx=true not set, non-determinism possible");
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-Dlog4j2.disable.jmx=true")))
            logger.warn("JVM Argument -Dlog4j2.disable.jmx=true not set, non-determinism possible");
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-Dlog4j.shutdownHookEnabled=false")))
            logger.warn("JVM Argument -Dlog4j.shutdownHookEnabled=false not set, non-determinism possible");
        if (!jvmArgs.stream().anyMatch(arg -> arg.equals("-Dcassandra.simulator.skiplog4jreload=true")))
            logger.warn("JVM Argument -Dcassandra.simulator.skiplog4jreload=true not set, non-determinism possible");
    }
}

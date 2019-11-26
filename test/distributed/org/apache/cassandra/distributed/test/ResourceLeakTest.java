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

package org.apache.cassandra.distributed.test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import javax.management.MBeanServer;

import org.junit.Ignore;
import org.junit.Test;

import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SigarLibrary;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;

/* Resource Leak Test - useful when tracking down issues with in-JVM framework cleanup.
 * All objects referencing the InstanceClassLoader need to be garbage collected or
 * the JVM runs out of metaspace. This test also calls out to lsof to check which
 * file handles are still opened.
 *
 * This is intended to be a burn type test where it is run outside of the test suites
 * when a problem is detected (like OutOfMetaspace exceptions).
 *
 * Currently this test demonstrates that the InstanceClassLoader is cleaned up (load up
 * the final hprof and check that the class loaders are not reachable from a GC root),
 * but it shows that the file handles for Data/Index files are being leaked.
 */
@Ignore
public class ResourceLeakTest extends DistributedTestBase
{
    // Parameters to adjust while hunting for leaks
    final int numTestLoops = 1;            // Set this value high to crash on leaks, or low when tracking down an issue.
    final boolean dumpEveryLoop = false;   // Dump heap & possibly files every loop
    final boolean dumpFileHandles = false; // Call lsof whenever dumping resources
    final boolean forceCollection = false; // Whether to explicitly force finalization/gc for smaller heap dumps
    final long finalWaitMillis = 0l;       // Number of millis to wait before final resource dump to give gc a chance

    static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    static final String when = format.format(Date.from(Instant.now()));

    static String outputFilename(String base, String description, String extension)
    {
        Path p = FileSystems.getDefault().getPath("build", "test",
                                                  String.join("-", when, base, description) + extension);
        return p.toString();
    }

    /**
     * Retrieves the process ID or <code>null</code> if the process ID cannot be retrieved.
     * @return the process ID or <code>null</code> if the process ID cannot be retrieved.
     *
     * (Duplicated from HeapUtils to avoid refactoring older releases where this test is useful).
     */
    private static Long getProcessId()
    {
        // Once Java 9 is ready the process API should provide a better way to get the process ID.
        long pid = SigarLibrary.instance.getPid();

        if (pid >= 0)
            return Long.valueOf(pid);

        return getProcessIdFromJvmName();
    }

    /**
     * Retrieves the process ID from the JVM name.
     * @return the process ID or <code>null</code> if the process ID cannot be retrieved.
     */
    private static Long getProcessIdFromJvmName()
    {
        // the JVM name in Oracle JVMs is: '<pid>@<hostname>' but this might not be the case on all JVMs
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        try
        {
            return Long.parseLong(jvmName.split("@")[0]);
        }
        catch (NumberFormatException e)
        {
            // ignore
        }
        return null;
    }

    static void dumpHeap(String description, boolean live) throws IOException
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
        server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        mxBean.dumpHeap(outputFilename("heap", description, ".hprof"), live);
    }

    static void dumpOpenFiles(String description) throws IOException, InterruptedException
    {
        long pid = getProcessId();
        ProcessBuilder map = new ProcessBuilder("/usr/sbin/lsof", "-p", Long.toString(pid));
        File output = new File(outputFilename("lsof", description, ".txt"));
        map.redirectOutput(output);
        map.redirectErrorStream(true);
        map.start().waitFor();
    }

    void dumpResources(String description) throws IOException, InterruptedException
    {
        dumpHeap(description, false);
        if (dumpFileHandles)
        {
            dumpOpenFiles(description);
        }
    }

    void doTest(int numClusterNodes, Consumer<InstanceConfig> updater) throws Throwable
    {
        for (int loop = 0; loop < numTestLoops; loop++)
        {
            System.out.println(String.format("========== Starting loop %03d ========", loop));
            try (Cluster cluster = Cluster.build(numClusterNodes).withConfig(updater).start())
            {
                if (cluster.get(1).config().has(GOSSIP)) // Wait for gossip to settle on the seed node
                    cluster.get(1).runOnInstance(() -> Gossiper.waitToSettle());

                init(cluster);
                String tableName = "tbl" + loop;
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + "(pk,ck,v) VALUES (0,0,0)", ConsistencyLevel.ALL);
                cluster.get(1).callOnInstance(() -> FBUtilities.waitOnFutures(Keyspace.open(KEYSPACE).flush()));
                if (dumpEveryLoop)
                {
                    dumpResources(String.format("loop%03d", loop));
                }
            }
            catch (Throwable tr)
            {
                System.out.println("Dumping resources for exception: " + tr.getMessage());
                tr.printStackTrace();
                dumpResources("exception");
            }
            if (forceCollection)
            {
                System.runFinalization();
                System.gc();
            }
            System.out.println(String.format("========== Completed loop %03d ========", loop));
        }
    }

    @Test
    public void looperTest() throws Throwable
    {
        doTest(1, config -> {});
        if (forceCollection)
        {
            System.runFinalization();
            System.gc();
            Thread.sleep(finalWaitMillis);
        }
        dumpResources("final");
    }

    @Test
    public void looperGossipNetworkTest() throws Throwable
    {
        doTest(2, config -> config.with(GOSSIP).with(NETWORK));
        if (forceCollection)
        {
            System.runFinalization();
            System.gc();
            Thread.sleep(finalWaitMillis);
        }
        dumpResources("final-gossip-network");
    }

    @Test
    public void looperNativeTest() throws Throwable
    {
        doTest(2, config -> config.with(NATIVE_PROTOCOL));
        if (forceCollection)
        {
            System.runFinalization();
            System.gc();
            Thread.sleep(finalWaitMillis);
        }
        dumpResources("final-native");
    }
}

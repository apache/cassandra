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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.jmx.JMXGetterCheckTest.testAllValidGetters;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

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

public class ResourceLeakTest extends TestBaseImpl
{
    // Parameters to adjust while hunting for leaks
    // numTestLoops should be >= 3, and numClusterNodes >= 2, when committed to source control.
    // This ensures the instance class loader assertions will fail quickly and reliably when a
    // new InstanceClassLoader leak is introduced.
    final int numTestLoops = 3;
    final int numClusterNodes = 2;
    final boolean dumpEveryLoop = true;   // Dump heap & possibly files every loop
    final boolean dumpFileHandles = false; // Call lsof whenever dumping resources
    final long finalWaitMillis = 0L;       // Number of millis to wait before final resource dump to give gc a chance

    static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    static final String when = format.format(Date.from(now()));

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
        long pid = FBUtilities.getSystemInfo().getPid();

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
        map.redirectOutput(output.toJavaIOFile());
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

    static void testJmx(Cluster cluster)
    {
        try
        {
            for (IInvokableInstance instance : cluster.get(1, cluster.size()))
            {
                IInstanceConfig config = instance.config();
                try (JMXConnector jmxc = JMXUtil.getJmxConnector(config, 5))
                {
                    MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                    // instances get their default domain set to their IP address, so us it
                    // to check that we are actually connecting to the correct instance
                    String defaultDomain = mbsc.getDefaultDomain();
                    Assert.assertThat(defaultDomain, startsWith(JMXUtil.getJmxHost(config) + ":" + config.jmxPort()));
                }
            }
            testAllValidGetters(cluster);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    void checkForInstanceClassLoaderLeaks(int maxAllowableInstances, int loop) throws IOException, InterruptedException {
        for (int i = 0; InstanceClassLoader.getApproximateLiveLoaderCount(true) > maxAllowableInstances && i < 120; i++) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        int approximateLiveLoaderCount = InstanceClassLoader.getApproximateLiveLoaderCount(true);
        if (approximateLiveLoaderCount > maxAllowableInstances) {
            dumpResources(String.format("InstanceClassLoader max reached at loop %d", loop));
            Assert.assertThat("InstanceClassLoader leak detected",
                    approximateLiveLoaderCount,
                    lessThanOrEqualTo(maxAllowableInstances));
        }
    }

    void doTest(Consumer<IInstanceConfig> updater) throws Throwable
    {
        doTest(updater, ignored -> {}, false);
    }

    void doTest(Consumer<IInstanceConfig> updater, Consumer<Cluster> actionToPerform, boolean shouldCheckForClassloaderLeaks) throws Throwable
    {
        for (int loop = 0; loop < numTestLoops; loop++)
        {
            System.out.println(String.format("========== Starting loop %03d ========", loop));
            Cluster.Builder builder = builder();
            try (Cluster cluster = (Cluster) builder.withNodes(numClusterNodes).withConfig(updater).start())
            {
                init(cluster);
                String tableName = "tbl" + loop;
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + "(pk,ck,v) VALUES (0,0,0)", ConsistencyLevel.ALL);
                cluster.get(1).flush(KEYSPACE);
                actionToPerform.accept(cluster);
                if (dumpEveryLoop)
                {
                    dumpResources(String.format("loop%03d", loop));
                }
                // We add 2 to the number of allowed classloaders to provide some wiggle room, as GC is non-deterministic
                // and some threads don't always shut down in time
                if (shouldCheckForClassloaderLeaks)
                {
                    checkForInstanceClassLoaderLeaks(numClusterNodes + 2, loop);
                }
            }
            catch (AssertionError ae) {
                throw ae;
            }
            catch (Throwable tr)
            {
                System.out.println("Dumping resources for exception: " + tr);
                tr.printStackTrace();
                dumpResources("exception");
            }
            System.runFinalization();
            System.gc();
            System.out.println(String.format("========== Completed loop %03d ========", loop));
        }
    }

    @Ignore("Only run if debugging an issue")
    @Test
    public void looperTest() throws Throwable
    {
        doTest(config -> {});
        System.runFinalization();
        System.gc();
        Thread.sleep(finalWaitMillis);
        dumpResources("final");
    }

    @Ignore("Only run if debugging an issue")
    @Test
    public void looperGossipNetworkTest() throws Throwable
    {
        doTest(config -> config.with(GOSSIP).with(NETWORK));
        System.runFinalization();
        System.gc();
        Thread.sleep(finalWaitMillis);
        dumpResources("final-gossip-network");
    }

    @Ignore("Only run if debugging an issue")
    @Test
    public void looperNativeTest() throws Throwable
    {
        doTest(config -> config.with(NATIVE_PROTOCOL));
        System.runFinalization();
        System.gc();
        Thread.sleep(finalWaitMillis);
        dumpResources("final-native");
    }

    @Ignore("Only run if debugging an issue")
    @Test
    public void looperJmxTest() throws Throwable
    {
        doTest(config -> config.with(JMX), ResourceLeakTest::testJmx, false);
        System.runFinalization();
        System.gc();
        Thread.sleep(finalWaitMillis);
        dumpResources("final-jmx");
    }

    /**
     * This test is enabled in an attempt to automatically catch InstanceClassloader leaks.
     * Depending on the type of leak, we may need to actually exercise functionality like JMX or Native
     * beyond just enabling the feature, so we use the "everything" test even though it may take longer to run.
     */
    @Test
    public void looperEverythingTest() throws Throwable
    {
        doTest(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL, JMX), // Exclude `BLANK_GOSSIP`
               cluster -> {
                   NativeProtocolTest.testNativeRequests(cluster);
                   testJmx(cluster);
               }, true);
        System.runFinalization();
        System.gc();
        Thread.sleep(finalWaitMillis);
        dumpResources("final-everything");
    }
}

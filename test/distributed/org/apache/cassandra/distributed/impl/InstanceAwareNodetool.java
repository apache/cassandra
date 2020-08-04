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

package org.apache.cassandra.distributed.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.mock.nodetool.InternalNodeProbeFactory;
import org.apache.cassandra.distributed.shared.AbstractBuilder;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.ToolRunner;

/**
 * Handy class for invoking nodetool against an instance, the difference between this
 * and nodetool method invocation on IInstance is that we can have an output from this one which is not
 * possible on IInstance nodetool invocation.
 * <p>
 * This is the gateway for testing the output of nodetool command and we can do various asserts on that, e.g.
 * if the output is of some format and that is not broken (even accidentally).
 * <p>
 * The actual usage is like:
 *
 * <pre>
 * {@code
 * public class GossipinfoTest extends TestBaseImpl
 * {
 *     @Test
 *     public void testGossipinfo() throws Exception
 *     {
 *         try (final InstanceAwareNodetool nodetool = new InstanceAwareNodetool(getCluster()))
 *         {
 *             String[] results = nodetool.execute("gossipinfo");
 *         }
 *     }
 * }
 * }</pre>
 */
public class InstanceAwareNodetool implements IIsolatedExecutor.SerializableCallable<String[]>, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceAwareNodetool.class);

    private final List<String> arguments = new ArrayList<>();
    private long timeoutInMillis = TimeUnit.SECONDS.toMillis(30);

    private Cluster cluster;

    public InstanceAwareNodetool()
    {

    }

    public Cluster getCluster()
    {
        return cluster;
    }

    public InstanceAwareNodetool(final AbstractBuilder<IInvokableInstance, Cluster, Cluster.Builder> builder) throws Exception
    {
        cluster = builder.start();
    }

    public static final class NodetoolWrapper extends NodeTool implements Serializable
    {
        public NodetoolWrapper(final INodeProbeFactory nodeProbeFactory)
        {
            super(nodeProbeFactory);
        }

        public static void main(final String... args)
        {
            synchronized (NodetoolWrapper.class)
            {
                new NodetoolWrapper(new InternalNodeProbeFactory(false)).execute(args);
            }
        }
    }

    public static String getStdout(String[] results)
    {
        return results[0];
    }

    public static String getStderr(String[] results)
    {
        return results[1];
    }

    public static Integer getExitCode(String[] results)
    {
        Assert.assertNotNull(results[2]);
        return Integer.parseInt(results[2]);
    }

    public static void assertEmptyStdout(String[] results)
    {
        Assert.assertTrue(results[0] == null || results[0].isEmpty());
    }

    public static void assertEmptyStderr(String[] results)
    {
        Assert.assertTrue(results[1] == null || results[1].isEmpty());
    }

    public static boolean finished(String[] results)
    {
        return Boolean.parseBoolean(results[3]);
    }

    public static void assertSuccessful(String[] results)
    {
        assertExitCode(results, 0);
        assertEmptyStderr(results);
        finished(results);
    }

    public static void assertExitCode(String[] results, int expectedExitCode)
    {
        Assert.assertNotNull("Command is still in progress, no exit code yet!", results[2]);
        Assert.assertEquals(Integer.parseInt(results[2]), expectedExitCode);
    }

    public void close() throws Exception
    {
        try
        {
            if (cluster != null)
            {
                cluster.close();
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException("Unable to close a cluster!", ex);
        }
    }

    /**
     * Executes a nodetool with given arguments on the first node in a cluster.
     *
     * @param args arguments for nodetool
     * @return an array with execution results
     */
    public synchronized String[] execute(String... args)
    {
        return execute(1, args);
    }

    public synchronized String[] execute(IInvokableInstance node, String... args)
    {
        final String[] results = node.callsOnInstance(new InstanceAwareNodetool().withArgs(args)).call();

        logger.info(InstanceAwareNodetool.getStdout(results));

        return results;
    }

    /**
     * Executes a nodetool command with given arguments on the n-th node in a cluster.
     *
     * @param n    number of a node in a cluster to run nodetool on, if n is lower or equal to 0, it is changed to 1.
     * @param args arguments for nodetool
     * @return an array with execution results
     */
    public synchronized String[] execute(int n, String... args)
    {
        // common mistake
        int resolvedNode = n <= 0 ? 1 : n;

        assert cluster != null;

        final IInvokableInstance node = cluster.get(resolvedNode);

        return execute(node, args);
    }

    /**
     * Executes respective nodetool command and returns an array with results. The first item is stdout, the second item
     * is stderr, the third item is the exit code and the fourth item is boolean telling if a command is still running
     * or not based on the timeout we expect that command to finish until.
     * <p>
     * Please use static method on this class to assert the state of this response or to get respective elements
     * conveniently.
     *
     * @return an array with the results.
     */
    public String[] call()
    {
        final List<String> command = new ArrayList<>();
        command.add(NodetoolWrapper.class.getName());
        command.addAll(arguments);

        final ToolRunner toolRunner = new ToolRunner.Runners().invokeClassAsTool(command);

        boolean finishedUntilTimeout;

        try
        {
            finishedUntilTimeout = toolRunner.waitFor(timeoutInMillis, TimeUnit.MILLISECONDS);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException("Invocation of nodetool command failed!", ex);
        }

        final String stdout = toolRunner.getStdout();
        final String stderr = toolRunner.getStderr();
        String exitCode = null;

        if (!toolRunner.isRunning())
        {
            exitCode = String.valueOf(toolRunner.getExitCode());
        }

        return new String[]{
        stdout,
        stderr,
        exitCode,
        String.valueOf(finishedUntilTimeout)
        };
    }

    public InstanceAwareNodetool withArgs(String... arguments)
    {
        final InstanceAwareNodetool nodetool = new InstanceAwareNodetool();
        nodetool.timeoutInMillis = this.timeoutInMillis;
        nodetool.arguments.addAll(Arrays.asList(arguments));
        return nodetool;
    }

    public InstanceAwareNodetool withTimeout(final long timeoutInMillis)
    {
        final InstanceAwareNodetool nodetool = new InstanceAwareNodetool();
        nodetool.timeoutInMillis = timeoutInMillis;
        nodetool.arguments.addAll(this.arguments);
        return nodetool;
    }
}

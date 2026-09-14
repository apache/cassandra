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

package org.apache.cassandra.inject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;

import com.google.common.collect.Lists;

import org.apache.commons.io.IOUtils;
import org.jboss.byteman.agent.install.Install;
import org.jboss.byteman.agent.submit.Submit;

import org.apache.cassandra.utils.Shared;

/**
 * Manages one Byteman agent and listener port per test JVM. {@link Shared} exposes the same port to each
 * in-JVM dtest class loader. The agent listens on {@code 127.0.0.1}. Policy installation is disabled because
 * JDK 24+ rejects {@code Policy.setPolicy}.
 */
@Shared
public final class BytemanAgentSupport
{
    private static final String HOST = "127.0.0.1";

    // BMUnit uses this property and default when it attaches the agent before this class does.
    private static final String BMUNIT_PORT_PROP = "org.jboss.byteman.contrib.bmunit.agent.port";
    private static final int BMUNIT_DEFAULT_PORT = 9091;

    private static volatile int port = -1;
    private static volatile boolean installed = false;

    private BytemanAgentSupport()
    {
    }

    /** Attaches the Byteman agent if the JVM does not have one. */
    public static synchronized void ensureInstalled()
    {
        if (installed)
            return;

        try
        {
            // Cassandra runs on JDK 11+.
            String pid = Long.toString(ProcessHandle.current().pid());
            if (Install.isAgentAttached(pid))
            {
                // Use the BMUnit port when another test attached the agent.
                port = Integer.getInteger(BMUNIT_PORT_PROP, BMUNIT_DEFAULT_PORT); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            }
            else
            {
                int chosen = freePort();
                // Add the agent to the bootstrap path so it can transform Runtime. Do not install a Policy.
                // Suppress installation output because callers may be capturing tool output.
                quietly(() -> Install.install(pid, true, false, HOST, chosen,
                                              new String[]{ "org.jboss.byteman.transform.all=true" }));
                port = chosen;
            }
            installed = true;
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to attach the Byteman agent", t);
        }
    }

    /** Returns a {@link Submit} for this JVM's Byteman agent. */
    public static Submit submitter()
    {
        ensureInstalled();
        return new Submit(HOST, port);
    }

    /** Submits rule text to this JVM's Byteman agent. */
    public static void submitRules(String ruleText)
    {
        ensureInstalled();
        try
        {
            quietly(() -> new Submit(HOST, port).addRulesFromResources(Lists.newArrayList(IOUtils.toInputStream(ruleText, "UTF-8"))));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to submit Byteman rules", e);
        }
    }

    /** Returns the agent host. */
    public static String host()
    {
        return HOST;
    }

    /** Returns the agent port, attaching the agent if needed. */
    public static int port()
    {
        ensureInstalled();
        return port;
    }

    @FunctionalInterface
    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    /** Runs {@code action} with standard output and error suppressed. */
    private static void quietly(ThrowingRunnable action) throws Exception
    {
        PrintStream out = System.out;
        PrintStream err = System.err;
        PrintStream nul = new PrintStream(OutputStream.nullOutputStream(), false, "UTF-8");
        System.setOut(nul);
        System.setErr(nul);
        try
        {
            action.run();
        }
        finally
        {
            System.setOut(out);
            System.setErr(err);
            nul.close();
        }
    }

    private static int freePort()
    {
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            return serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}

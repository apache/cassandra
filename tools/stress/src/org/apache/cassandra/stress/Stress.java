/**
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
package org.apache.cassandra.stress;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.MultiResultLogger;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WindowsTimer;

public final class Stress
{

    /**
     * Known issues:
     * - uncertainty/stderr assumes op-rates are normally distributed. Due to GC (and possibly latency stepping from
     * different media, though the variance of request ratio across media should be normally distributed), they are not.
     * Should attempt to account for pauses in stderr calculation, possibly by assuming these pauses are a separate
     * normally distributed occurrence
     * - Under very mixed work loads, the uncertainty calculations and op/s reporting really don't mean much. Should
     * consider breaking op/s down per workload, or should have a lower-bound on inspection interval based on clustering
     * of operations and thread count.
     *
     *
     * Future improvements:
     * - Configurable connection compression
     * - Java driver support
     * - Per column data generators
     * - Automatic column/schema detection if provided with a CF
     * - target rate produces a very steady work rate, and if we want to simulate a real op rate for an
     *   application we should have some variation in the actual op rate within any time-slice.
     * - auto rate should vary the thread count based on performance improvement, potentially starting on a very low
     *   thread count with a high error rate / low count to get some basic numbers
     */

    private static volatile boolean stopped = false;

    public static void main(String[] arguments) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12343
        if (FBUtilities.isWindows)
            WindowsTimer.startTimerPeriod(1);

        int exitCode = run(arguments);

        if (FBUtilities.isWindows)
            WindowsTimer.endTimerPeriod(1);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10340

        System.exit(exitCode);
    }


    private static int run(String[] arguments)
    {
        try
        {
            DatabaseDescriptor.clientInitialization();

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9864
            final StressSettings settings;
            try
            {
                settings = StressSettings.parse(arguments);
                if (settings == null)
                    return 0; // special settings action
            }
            catch (IllegalArgumentException e)
            {
                System.out.printf("%s%n", e.getMessage());
                printHelpMessage();
                return 1;
            }

            MultiResultLogger logout = settings.log.getOutput();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12237

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11914
            if (! settings.log.noSettings)
            {
                settings.printSettings(logout);
            }

            if (settings.graph.inGraphMode())
            {
                logout.addStream(new PrintStream(settings.graph.temporaryLogFile));
            }

            if (settings.sendToDaemon != null)
            {
                Socket socket = new Socket(settings.sendToDaemon, 2159);

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                BufferedReader inp = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                Runtime.getRuntime().addShutdownHook(new ShutDown(socket, out));

                out.writeObject(settings);

                String line;

                try
                {
                    while (!socket.isClosed() && (line = inp.readLine()) != null)
                    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-4188
                        if (line.equals("END") || line.equals("FAILURE"))
                        {
                            out.writeInt(1);
                            break;
                        }

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6199
                        logout.println(line);
                    }
                }
                catch (SocketException e)
                {
                    if (!stopped)
                        e.printStackTrace();
                }

                out.close();
                inp.close();

                socket.close();
            }
            else
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6199
                StressAction stressAction = new StressAction(settings, logout);
                stressAction.run();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7918
                logout.flush();
                if (settings.graph.inGraphMode())
                    new StressGraph(settings, arguments).generateGraph();
            }

        }
        catch (Throwable t)
        {
            t.printStackTrace();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10340
            return 1;
        }

        return 0;
    }

    /**
     * Printing out help message
     */
    public static void printHelpMessage()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6199
        StressSettings.printHelp();
    }

    private static class ShutDown extends Thread
    {
        private final Socket socket;
        private final ObjectOutputStream out;

        public ShutDown(Socket socket, ObjectOutputStream out)
        {
            this.out = out;
            this.socket = socket;
        }

        public void run()
        {
            try
            {
                if (!socket.isClosed())
                {
                    System.out.println("Control-C caught. Canceling running action and shutting down...");

                    out.writeInt(1);
                    out.close();

                    stopped = true;
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

}

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

import org.apache.commons.cli.Option;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;

public final class Stress
{
    public static enum Operations
    {
        INSERT, READ, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET, COUNTER_ADD, COUNTER_GET
    }

    public static Session session;
    public static Random randomizer = new Random();
    private static volatile boolean stopped = false;

    public static void main(String[] arguments) throws Exception
    {
        try
        {
            session = new Session(arguments);
        }
        catch (IllegalArgumentException e)
        {
            printHelpMessage();
            return;
        }

        PrintStream outStream = session.getOutputStream();

        if (session.sendToDaemon != null)
        {
            Socket socket = new Socket(session.sendToDaemon, 2159);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            BufferedReader inp = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            Runtime.getRuntime().addShutdownHook(new ShutDown(socket, out));

            out.writeObject(session);

            String line;

            try
            {
                while (!socket.isClosed() && (line = inp.readLine()) != null)
                {
                    if (line.equals("END"))
                    {
                        out.writeInt(1);
                        break;
                    }

                    outStream.println(line);
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
            new StressAction(session, outStream).run();
        }
    }

    /**
     * Printing out help message
     */
    public static void printHelpMessage()
    {
        System.out.println("Usage: ./bin/stress [options]\n\nOptions:");

        for(Object o : Session.availableOptions.getOptions())
        {
            Option option = (Option) o;
            String upperCaseName = option.getLongOpt().toUpperCase();
            System.out.println(String.format("-%s%s, --%s%s%n\t\t%s%n", option.getOpt(), (option.hasArg()) ? " "+upperCaseName : "",
                                                            option.getLongOpt(), (option.hasArg()) ? "="+upperCaseName : "", option.getDescription()));
        }
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

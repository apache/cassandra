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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.commons.cli.*;

public class StressServer
{
    private static final Options availableOptions = new Options();

    static
    {
        availableOptions.addOption("h", "host", true, "Host to listen for connections.");
    }

    public static void main(String[] args) throws Exception
    {
        ServerSocket serverSocket = null;
        CommandLineParser parser  = new PosixParser();

        InetAddress address = InetAddress.getByName("127.0.0.1");

        try
        {
            CommandLine cmd = parser.parse(availableOptions, args);

            if (cmd.hasOption("h"))
            {
                address = InetAddress.getByName(cmd.getOptionValue("h"));
            }
        }
        catch (ParseException e)
        {
            System.err.printf("Usage: ./bin/stressd start|stop|status [-h <host>]");
            System.exit(1);
        }

        try
        {
            serverSocket = new ServerSocket(2159, 0, address);
        }
        catch (IOException e)
        {
            System.err.printf("Could not listen on port: %s:2159.%n", address.getHostAddress());
            System.exit(1);
        }

        for (;;)
            new StressThread(serverSocket.accept()).start();
    }

    public static class StressThread extends Thread
    {
        private final Socket socket;

        public StressThread(Socket client)
        {
            this.socket = client;
        }

        public void run()
        {
            try
            {
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                PrintStream out = new PrintStream(socket.getOutputStream());

                StressAction action = new StressAction((StressSettings) in.readObject(), out);
                Thread actionThread = new Thread(action);
                actionThread.start();

                while (actionThread.isAlive())
                {
                    try
                    {
                        if (in.readInt() == 1)
                        {
                            actionThread.interrupt();
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        // continue without problem
                    }
                }

                out.close();
                in.close();
                socket.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e.getMessage(), e);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

    }

}

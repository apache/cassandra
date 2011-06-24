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
package org.apache.cassandra.stress.server;

import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.StressAction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

public class StressThread extends Thread
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

            StressAction action = new StressAction((Session) in.readObject(), out);
            action.start();

            while (action.isAlive())
            {
                try
                {
                    if (in.readInt() == 1)
                    {
                        action.stopAction();
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

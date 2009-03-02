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

package org.apache.cassandra.cli;

import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;

import jline.*;
import java.io.*;
import java.util.*;

import org.apache.cassandra.service.Cassandra;

//
// Cassandra Command Line Interface (CLI) Main
//
public class CliMain
{
    public final static String PROMPT = "cassandra";
    public final static String HISTORYFILE = ".cassandra.history";

    private static TTransport transport_ = null;
    private static Cassandra.Client thriftClient_ = null;
    private static CliSessionState css_ = new CliSessionState();
    private static CliClient cliClient_;

    // Establish a thrift connection to cassandra instance
    public static void connect(String server, int port)
    {
        TSocket socket = new TSocket(server, port);

        if (transport_ != null)
            transport_.close();

        transport_ = socket;

        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport_, false, false);
        Cassandra.Client cassandraClient = new Cassandra.Client(binaryProtocol);

        try
        {
            transport_.open();
        }
        catch(Exception e)
        {
            // Should move this to Log4J as well probably...
            System.err.println("Exception " + e.getMessage());            
            e.printStackTrace();
        }

        thriftClient_ = cassandraClient;
        cliClient_ = new CliClient(css_, thriftClient_);

        css_.out.printf("Connected to %s/%d\n", server, port);
    }

    // Disconnect thrift connection to cassandra instance
    public static void disconnect()
    {
        if (transport_ != null)
        {
            transport_.close();
            transport_ = null;
        }
    }

    private static void printBanner()
    {
        css_.out.println("Welcome to cassandra CLI.\n");
        css_.out.println("Type 'help' or '?' for help. Type 'quit' or 'exit' to quit.");
    }

    public static boolean isConnected()
    {
        if (thriftClient_ == null)
        {
            css_.out.println("Not connected to a cassandra instance.");
            return false;
        }
        return true;
    }
    
    private static void processServerQuery(String query)
    {
        if (!isConnected())
            return;

        try
        {
            cliClient_.executeQueryOnServer(query);
        }
        catch(Exception e)
        {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
        return;
    }

    private static void processCLIStmt(String query)
    {
        try
        {
            cliClient_.executeCLIStmt(query);
        }
        catch(Exception e)
        {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
        return;
    }

    private static void processLine(String line)
    {
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.hasMoreTokens())
        {
            // Use first token for now to determine if this statement is
            // a CQL statement. Technically, the line could start with
            // a comment token followed by a CQL statement. That case
            // isn't handled right now.
            String token = tokenizer.nextToken().toUpperCase();
            if (token.startsWith("GET")
                || token.startsWith("SELECT")
                || token.startsWith("SET")
                || token.startsWith("DELETE")
                || token.startsWith("EXPLAIN")) // explain plan statement
            {
                // these are CQL Statements that are compiled and executed on server-side
                processServerQuery(line);
            }
            else 
            {
                // These are CLI statements processed locally
                processCLIStmt(line);
            }
        }
    } 

    public static void main(String args[]) throws IOException  
    {
        // process command line args
        CliOptions cliOptions = new CliOptions();
        cliOptions.processArgs(css_, args);

        // connect to cassandra server if host argument specified.
        if (css_.hostName != null)
        {
            connect(css_.hostName, css_.thriftPort);
        }
        else
        {
            // If not, client must connect explicitly using the "connect" CLI statement.
            cliClient_ = new CliClient(css_, null);
        }

        ConsoleReader reader = new ConsoleReader(); 
        reader.setBellEnabled(false);

        String historyFile = System.getProperty("user.home") + File.separator  + HISTORYFILE;

        reader.setHistory(new History(new File(historyFile)));

        printBanner();

        String line;
        while ((line = reader.readLine(PROMPT+"> ")) != null)
        {
            processLine(line);
        }
    }
}

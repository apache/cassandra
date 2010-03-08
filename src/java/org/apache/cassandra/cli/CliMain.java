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

import jline.ConsoleReader;
import jline.History;
import org.apache.cassandra.auth.SimpleAuthenticator;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.cassandra.db.Table.SYSTEM_TABLE;

/**
 * Cassandra Command Line Interface (CLI) Main
 */
public class CliMain
{
    public final static String PROMPT = "cassandra";
    public final static String HISTORYFILE = ".cassandra.history";

    private static TTransport transport_ = null;
    private static Cassandra.Client thriftClient_ = null;
    private static CliSessionState css_ = new CliSessionState();
    private static CliClient cliClient_;
    private static CliCompleter completer_ = new CliCompleter();

    /**
     * Establish a thrift connection to cassandra instance
     *
     * @param server - hostname or IP of the server
     * @param port   - Thrift port number
     */
    public static void connect(String server, int port)
    {

        TSocket socket = new TSocket(server, port);

        if (transport_ != null)
            transport_.close();

        if (css_.framed)
        {
            transport_ = new TFramedTransport(socket);
        }
        else 
        {
            transport_ = socket;
        }

        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport_, false, false);
        Cassandra.Client cassandraClient = new Cassandra.Client(binaryProtocol);

        try
        {
            transport_.open();
        }
        catch (Exception e)
        {
            // Should move this to Log4J as well probably...
            css_.err.format("Exception connecting to %s/%d - %s\n", server, port, e.getMessage());

            if (css_.debug)
                e.printStackTrace();

            return;
        }

        thriftClient_ = cassandraClient;
        cliClient_ = new CliClient(css_, thriftClient_);
        
        // Authenticate
        Map<String, String> credentials = new HashMap<String, String>();
        credentials.put(SimpleAuthenticator.USERNAME_KEY, css_.username);
        credentials.put(SimpleAuthenticator.PASSWORD_KEY, css_.password);
        AuthenticationRequest authRequest = new AuthenticationRequest(credentials);
        try 
        {
            thriftClient_.login(css_.keyspace, authRequest);
        } 
        catch (AuthenticationException e) 
        {
            css_.err.println("Exception during authentication to the cassandra node, " +
            		"verify you are using correct credentials.");
            return;
        } 
        catch (AuthorizationException e) 
        {
            css_.err.println("You are not authorized to use keyspace: " + css_.keyspace);
            return;
        } 
        catch (TException e) 
        {
            if (css_.debug)
                e.printStackTrace();
            
            css_.err.println("Login failure. Did you specify 'keyspace', 'username' and 'password'?");
            return;
        }
        
        // Lookup the cluster name, this is to make it clear which cluster the user is connected to
        String clusterName;

        try
        {
            clusterName = thriftClient_.get_string_property("cluster name");
        }
        catch (Exception e)
        {

            css_.err.println("Exception retrieving information about the cassandra node, check you have connected to the thrift port.");

            if (css_.debug)
                e.printStackTrace();

            return;
        }

        // Extend the completer with keyspace and column family data.
        try
        {
            for (String keyspace : thriftClient_.get_string_list_property("keyspaces"))
            {
                // Ignore system column family
                if (keyspace.equals(SYSTEM_TABLE))
                    continue;

                for (String cf : cliClient_.getCFMetaData(keyspace).keySet())
                {
                    for (String cmd : completer_.getKeyspaceCommands())
                        completer_.addCandidateString(String.format("%s %s.%s", cmd, keyspace, cf));
                }
            }
        }
        catch (Exception e)
        {
            // Yes, we really do want to ignore any exceptions encountered here.
            if (css_.debug)
                e.printStackTrace();

            return;
        }

        css_.out.printf("Connected to: \"%s\" on %s/%d%n", clusterName, server, port);
    }

    /**
     * Disconnect thrift connection to cassandra instance
     */
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

    /**
     * Checks whether the thrift client is connected.
     */
    public static boolean isConnected()
    {
        if (thriftClient_ == null)
        {
            css_.out.println("Not connected to a cassandra instance.");
            return false;
        }
        return true;
    }

    private static void processCLIStmt(String query)
    {
        try
        {
            cliClient_.executeCLIStmt(query);
        }
        catch (InvalidRequestException ire)
        {
            css_.err.println(ire.why);
            if (css_.debug)
                ire.printStackTrace();
        }
        catch (Exception e)
        {
            css_.err.println("Exception " + e.getMessage());
            if (css_.debug)
                e.printStackTrace();

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
        reader.addCompletor(completer_);
        reader.setBellEnabled(false);

        String historyFile = System.getProperty("user.home") + File.separator + HISTORYFILE;

        try
        {
            History history = new History(new File(historyFile));
            reader.setHistory(history);
        }
        catch (IOException exp)
        {
            css_.err.printf("Unable to open %s for writing%n", historyFile);
        }

        printBanner();

        String line;
        while ((line = reader.readLine(PROMPT + "> ")) != null)
        {
            processCLIStmt(line);
        }
    }
}

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import jline.ConsoleReader;
import jline.History;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Cassandra Command Line Interface (CLI) Main
 */
public class CliMain
{
    public final static String HISTORYFILE = ".cassandra.history";

    private static TTransport transport = null;
    private static Cassandra.Client thriftClient = null;
    public  static CliSessionState sessionState = new CliSessionState();
    private static CliClient cliClient;
    private static CliCompleter completer = new CliCompleter();
    private static int lineNumber = 1;

    /**
     * Establish a thrift connection to cassandra instance
     *
     * @param server - hostname or IP of the server
     * @param port   - Thrift port number
     */
    public static void connect(String server, int port)
    {

        TSocket socket = new TSocket(server, port);

        if (transport != null)
            transport.close();

        if (sessionState.framed)
        {
            transport = new TFramedTransport(socket);
        }
        else 
        {
            transport = socket;
        }

        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport, true, true);
        Cassandra.Client cassandraClient = new Cassandra.Client(binaryProtocol);

        try
        {
            transport.open();
        }
        catch (Exception e)
        {
            e.printStackTrace(sessionState.err);

            String error = (e.getCause() == null) ? e.getMessage() : e.getCause().getMessage();
            throw new RuntimeException("Exception connecting to " + server + "/" + port + ". Reason: " + error + ".");
        }

        thriftClient = cassandraClient;
        cliClient = new CliClient(sessionState, thriftClient);
        
        if ((sessionState.username != null) && (sessionState.password != null))
        {
            // Authenticate
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(IAuthenticator.USERNAME_KEY, sessionState.username);
            credentials.put(IAuthenticator.PASSWORD_KEY, sessionState.password);
            AuthenticationRequest authRequest = new AuthenticationRequest(credentials);
            try
            {
                thriftClient.login(authRequest);
                cliClient.setUsername(sessionState.username);
            }
            catch (AuthenticationException e)
            {
                thriftClient = null;
                sessionState.err.println("Exception during authentication to the cassandra node, " +
                		"Verify the keyspace exists, and that you are using the correct credentials.");
                return;
            }
            catch (AuthorizationException e)
            {
                thriftClient = null;
                sessionState.err.println("You are not authorized to use keyspace: " + sessionState.keyspace);
                return;
            }
            catch (TException e)
            {
                thriftClient = null;
                sessionState.err.println("Login failure. Did you specify 'keyspace', 'username' and 'password'?");
                return;
            }
        }
        
        if (sessionState.keyspace != null)
        {
            try
            {
                sessionState.keyspace = CliCompiler.getKeySpace(sessionState.keyspace, thriftClient.describe_keyspaces());;
                thriftClient.set_keyspace(sessionState.keyspace);
                cliClient.setKeySpace(sessionState.keyspace);
                updateCompletor(CliUtils.getCfNamesByKeySpace(cliClient.getKSMetaData(sessionState.keyspace)));
            }
            catch (InvalidRequestException e)
            {
                sessionState.err.println("Keyspace " + sessionState.keyspace + " not found");
                return;
            }
            catch (TException e)
            {
                sessionState.err.println("Did you specify 'keyspace'?");
                return;
            }
            catch (NotFoundException e)
            {
                sessionState.err.println("Keyspace " + sessionState.keyspace + " not found");
                return;
            }
        }

        // Lookup the cluster name, this is to make it clear which cluster the user is connected to
        String clusterName;

        try
        {
            clusterName = thriftClient.describe_cluster_name();
        }
        catch (Exception e)
        {
            sessionState.err.println("Exception retrieving information about the cassandra node, check you have connected to the thrift port.");

            e.printStackTrace(sessionState.err);

            return;
        }

        sessionState.out.printf("Connected to: \"%s\" on %s/%d%n", clusterName, server, port);
    }

    /**
     * Disconnect thrift connection to cassandra instance
     */
    public static void disconnect()
    {
        if (transport != null)
        {
            transport.close();
            transport = null;
        }
    }

    /**
     * Checks whether the thrift client is connected.
     * @return boolean - true when connected, false otherwise
     */
    public static boolean isConnected()
    {
        if (thriftClient == null)
        {
            sessionState.out.println("Not connected to a cassandra instance.");
            return false;
        }
        return true;
    }
    
    public static void updateCompletor(Set<String> candidates)
    {
        Set<String> actions = new HashSet<String>();
        for (String cf : candidates)
        {
            for (String cmd : completer.getKeyspaceCommands())
                actions.add(String.format("%s %s", cmd, cf));
        }
        
        String[] strs = Arrays.copyOf(actions.toArray(), actions.toArray().length, String[].class);
        
        completer.setCandidateStrings(strs);
    }

    public static void processStatement(String query) throws CharacterCodingException, ClassNotFoundException, TException, TimedOutException, SchemaDisagreementException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException
    {
        cliClient.executeCLIStatement(query);
    }

    public static void processStatementInteractive(String query)
    {
        try
        {
            cliClient.executeCLIStatement(query);
        }
        catch (Exception e)
        {
            String errorTemplate = sessionState.inFileMode() ? "Line " + lineNumber + " => " : "";

            Throwable exception = (e.getCause() == null) ? e : e.getCause();
            String message = (exception instanceof InvalidRequestException) ? ((InvalidRequestException) exception).getWhy() : e.getMessage();

            sessionState.err.println(errorTemplate + message);

            if (!(e instanceof RuntimeException))
                e.printStackTrace(sessionState.err);

            if (sessionState.batch || sessionState.inFileMode())
            {
                System.exit(4);
            }
        }
        finally
        {
            lineNumber++;
        }
    }

    public static void main(String args[]) throws IOException
    {
        // process command line arguments
        CliOptions cliOptions = new CliOptions();
        cliOptions.processArgs(sessionState, args);

        // connect to cassandra server if host argument specified.
        if (sessionState.hostName != null)
        {
            try
            {
                connect(sessionState.hostName, sessionState.thriftPort);   
            }
            catch (RuntimeException e)
            {
                sessionState.err.println(e.getMessage());
                System.exit(-1);
            }
        }
        
        if ( cliClient == null )
        {
            // Connection parameter was either invalid or not present.
            // User must connect explicitly using the "connect" CLI statement.
            cliClient = new CliClient(sessionState, null);
        }

        // load statements from file and process them
        if (sessionState.inFileMode())
        {
            FileReader fileReader;

            try
            {
                fileReader = new FileReader(sessionState.filename);
            }
            catch (IOException e)
            {
                sessionState.err.println(e.getMessage());
                return;
            }

            evaluateFileStatements(new BufferedReader(fileReader));
            return;
        }

        ConsoleReader reader = new ConsoleReader();
        
        if (!sessionState.batch)
        {
            reader.addCompletor(completer);
            reader.setBellEnabled(false);
            
            String historyFile = System.getProperty("user.home") + File.separator + HISTORYFILE;

            try
            {
                History history = new History(new File(historyFile));
                reader.setHistory(history);
            }
            catch (IOException exp)
            {
                sessionState.err.printf("Unable to open %s for writing %n", historyFile);
            }
        }
        else if (!sessionState.verbose) // if in batch mode but no verbose flag
        {
            sessionState.out.close();
        }

        cliClient.printBanner();

        String prompt;
        String line = "";
        String currentStatement = "";
        boolean inCompoundStatement = false;

        while (line != null)
        {
            prompt = (inCompoundStatement) ? "...\t" : getPrompt(cliClient);

            try
            {
                line = reader.readLine(prompt);
            }
            catch (IOException e)
            {
                // retry on I/O Exception
            }

            if (line == null)
                return;

            line = line.trim();

            // skipping empty and comment lines
            if (line.isEmpty() || line.startsWith("--"))
                continue;

            currentStatement += line;

            if (line.endsWith(";") || line.equals("?"))
            {
                processStatementInteractive(currentStatement);
                currentStatement = "";
                inCompoundStatement = false;
            }
            else
            {
                currentStatement += " "; // ready for new line
                inCompoundStatement = true;
            }
        }
    }

    private static void evaluateFileStatements(BufferedReader reader) throws IOException
    {
        String line = "";
        String currentStatement = "";

        boolean commentedBlock = false;

        while ((line = reader.readLine()) != null)
        {
            line = line.trim();

            // skipping empty and comment lines
            if (line.isEmpty() || line.startsWith("--"))
                continue;

            if (line.startsWith("/*"))
                commentedBlock = true;

            if (line.startsWith("*/") || line.endsWith("*/"))
            {
                commentedBlock = false;
                continue;
            }

            if (commentedBlock) // skip commented lines
                continue;

            currentStatement += line;

            if (line.endsWith(";"))
            {
                processStatementInteractive(currentStatement);
                currentStatement = "";
            }
            else
            {
                currentStatement += " "; // ready for new line
            }
        }
    }

    /**
     * Returns prompt for current connection
     * @param client - currently connected client
     * @return String - prompt with username and keyspace (if any)
     */
    private static String getPrompt(CliClient client)
    {
        return "[" + client.getUsername() + "@" + client.getKeySpace() + "] ";
    }

}

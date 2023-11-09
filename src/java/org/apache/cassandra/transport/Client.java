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
package org.apache.cassandra.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.base.Splitter;

import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;

public class Client extends SimpleClient
{
    private final SimpleEventHandler eventHandler = new SimpleEventHandler();

    public Client(String host, int port, ProtocolVersion version, EncryptionOptions encryptionOptions)
    {
        super(host, port, version, version.isBeta(), new EncryptionOptions(encryptionOptions).applyConfig());
        setEventHandler(eventHandler);
    }

    public void run() throws IOException
    {
        // Start the connection attempt.
        System.out.print("Connecting...");
        establishConnection();
        System.out.println();

        // Read commands from the stdin.
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        for (;;)
        {
            Event event;
            while ((event = eventHandler.queue.poll()) != null)
            {
                System.out.println("<< " + event);
            }

            System.out.print(">> ");
            System.out.flush();
            String line = in.readLine();
            if (line == null)
            {
                break;
            }
            Message.Request req = parseLine(line.trim());
            if (req == null)
            {
                System.out.println("! Error parsing line.");
                continue;
            }

            try
            {
                Message.Response resp = execute(req);
                System.out.println("-> " + resp);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                System.err.println("ERROR: " + e.getMessage());
            }
        }

        close();
    }

    private Message.Request parseLine(String line)
    {
        Splitter splitter = Splitter.on(' ').trimResults().omitEmptyStrings();
        Iterator<String> iter = splitter.split(line).iterator();
        if (!iter.hasNext())
            return null;
        String msgType = iter.next().toUpperCase();
        if (msgType.equals("STARTUP"))
        {
            Map<String, String> options = new HashMap<String, String>();
            options.put(StartupMessage.CQL_VERSION, "3.0.0");
            while (iter.hasNext())
            {
               String next = iter.next();
               if (next.toLowerCase().equals("snappy"))
               {
                   options.put(StartupMessage.COMPRESSION, "snappy");
                   connection.setCompressor(Compressor.SnappyCompressor.instance);
               }
               if (next.toLowerCase().equals("lz4"))
               {
                   options.put(StartupMessage.COMPRESSION, "lz4");
                   connection.setCompressor(Compressor.LZ4Compressor.instance);
               }
               if (next.toLowerCase().equals("throw_on_overload"))
               {
                   options.put(StartupMessage.THROW_ON_OVERLOAD, "1");
                   connection.setThrowOnOverload(true);
               }
            }
            return new StartupMessage(options);
        }
        else if (msgType.equals("QUERY"))
        {
            line = line.substring(6);
            // Ugly hack to allow setting a page size, but that's playground code anyway
            String query = line;
            int pageSize = -1;
            if (line.matches(".+ !\\d+$"))
            {
                int idx = line.lastIndexOf('!');
                query = line.substring(0, idx-1);
                try
                {
                    pageSize = Integer.parseInt(line.substring(idx+1, line.length()));
                }
                catch (NumberFormatException e)
                {
                    return null;
                }
            }
            return new QueryMessage(query, QueryOptions.create(ConsistencyLevel.ONE, Collections.<ByteBuffer>emptyList(), false, pageSize, null, null, version, null));
        }
        else if (msgType.equals("PREPARE"))
        {
            String query = line.substring(8);
            return new PrepareMessage(query, null);
        }
        else if (msgType.equals("EXECUTE"))
        {
            try
            {
                byte[] preparedStatementId = Hex.hexToBytes(iter.next());
                byte[] resultMetadataId = Hex.hexToBytes(iter.next());

                List<ByteBuffer> values = new ArrayList<ByteBuffer>();
                while(iter.hasNext())
                {
                    String next = iter.next();
                    ByteBuffer bb;
                    try
                    {
                        int v = Integer.parseInt(next);
                        bb = Int32Type.instance.decompose(v);
                    }
                    catch (NumberFormatException e)
                    {
                        bb = UTF8Type.instance.decompose(next);
                    }
                    values.add(bb);
                }
                return new ExecuteMessage(MD5Digest.wrap(preparedStatementId), MD5Digest.wrap(resultMetadataId), QueryOptions.forInternalCalls(ConsistencyLevel.ONE, values));
            }
            catch (Exception e)
            {
                return null;
            }
        }
        else if (msgType.equals("OPTIONS"))
        {
            return new OptionsMessage();
        }
        else if (msgType.equals("AUTHENTICATE"))
        {
            Map<String, String> credentials = readCredentials(iter);
            if(!credentials.containsKey(PasswordAuthenticator.USERNAME_KEY) || !credentials.containsKey(PasswordAuthenticator.PASSWORD_KEY))
            {
                System.err.println("[ERROR] Authentication requires both 'username' and 'password'");
                return null;
            }
            return new AuthResponse(encodeCredentialsForSasl(credentials));
        }
        else if (msgType.equals("REGISTER"))
        {
            String type = line.substring(9).toUpperCase();
            try
            {
                return new RegisterMessage(Collections.singletonList(Enum.valueOf(Event.Type.class, type)));
            }
            catch (IllegalArgumentException e)
            {
                System.err.println("[ERROR] Unknown event type: " + type);
                return null;
            }
        }
        return null;
    }

    private Map<String, String> readCredentials(Iterator<String> iter)
    {
        final Map<String, String> credentials = new HashMap<String, String>();
        while (iter.hasNext())
        {
            String next = iter.next();
            String[] kv = next.split("=");
            if (kv.length != 2)
            {
                System.err.println("[ERROR] Default authentication requires username & password");
                return null;
            }
            credentials.put(kv[0], kv[1]);
        }
        return credentials;
    }

    private byte[] encodeCredentialsForSasl(Map<String, String> credentials)
    {
        byte[] username = credentials.get(PasswordAuthenticator.USERNAME_KEY).getBytes(StandardCharsets.UTF_8);
        byte[] password = credentials.get(PasswordAuthenticator.PASSWORD_KEY).getBytes(StandardCharsets.UTF_8);
        byte[] initialResponse = new byte[username.length + password.length + 2];
        initialResponse[0] = 0;
        System.arraycopy(username, 0, initialResponse, 1, username.length);
        initialResponse[username.length + 1] = 0;
        System.arraycopy(password, 0, initialResponse, username.length + 2, password.length);
        return initialResponse;
    }

    public static void main(String[] args) throws Exception
    {
        DatabaseDescriptor.clientInitialization();

        // Print usage if no argument is specified.
        if (args.length < 2 || args.length > 3)
        {
            System.err.println("Usage: " + Client.class.getSimpleName() + " <host> <port> [<version>]");
            return;
        }

        // Parse options.
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        ProtocolVersion version = args.length == 3 ? ProtocolVersion.decode(Integer.parseInt(args[2]), DatabaseDescriptor.getNativeTransportAllowOlderProtocols()) : ProtocolVersion.CURRENT;

        EncryptionOptions encryptionOptions = new EncryptionOptions().applyConfig();
        System.out.println("CQL binary protocol console " + host + "@" + port + " using native protocol version " + version);

        try (Client client = new Client(host, port, version, encryptionOptions))
        {
            client.run();
        }
        System.exit(0);
    }
}

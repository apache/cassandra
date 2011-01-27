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

import org.apache.cassandra.tools.NodeProbe;

import java.io.InputStream;
import java.io.PrintStream;

/**
 * Used to hold the state for the CLI.
 */
public class CliSessionState
{

    public String  hostName;      // cassandra server name
    public int     thriftPort;    // cassandra server's thrift port
    public boolean framed = true; // cassandra server's framed transport 
    public boolean debug = false; // print stack traces when errors occur in the CLI
    public String  username;      // cassandra login name (if SimpleAuthenticator is used)
    public String  password;      // cassandra login password (if SimpleAuthenticator is used)
    public String  keyspace;      // cassandra keyspace user is authenticating
    public boolean batch = false; // enable/disable batch processing mode
    public String  filename = ""; // file to read commands from
    public int     jmxPort = 7199;// JMX service port
    public boolean verbose = false; // verbose output
    public int     schema_mwt;    // Schema migration wait time (secs.)
    /*
     * Streams to read/write from
     */
    public InputStream in;
    public PrintStream out;
    public PrintStream err;

    public CliSessionState()
    {
        in = System.in;
        out = System.out;
        err = System.err;
    }

    public void setOut(PrintStream newOut)
    {
        this.out = newOut;   
    }

    public void setErr(PrintStream newErr)
    {
        this.err = newErr;
    }

    public boolean inFileMode()
    {
        return !this.filename.isEmpty();
    }

    public NodeProbe getNodeProbe()
    {
        try
        {
            return new NodeProbe(hostName, jmxPort);
        }
        catch (Exception e)
        {
            err.printf("WARNING: Could not connect to the JMX on %s:%d, information won't be shown.%n%n", hostName, jmxPort);
        }

        return null;
    }
}

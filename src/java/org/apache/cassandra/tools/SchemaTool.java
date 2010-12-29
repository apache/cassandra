package org.apache.cassandra.tools;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;

public class SchemaTool
{
    public static void main(String[] args)
    throws NumberFormatException, IOException, InterruptedException, ConfigurationException
    {
        if (args.length < 3 || args.length > 3)
            usage();
        
        String host = args[0];
        int port = 0;
        
        try
        {
            port = Integer.parseInt(args[1]);
        }
        catch (NumberFormatException e)
        {
            System.err.println("Port must be a number.");
            System.exit(1);
        }
        
        if ("import".equals(args[2]))
            new NodeProbe(host, port).loadSchemaFromYAML();
        else if ("export".equals(args[2]))
            System.out.println(new NodeProbe(host, port).exportSchemaToYAML());
        else
            usage();
    }
    
    private static void usage()
    {
        System.err.printf("java %s <host> <port> import|export%n", SchemaTool.class.getName());
        System.exit(1);
    }
}

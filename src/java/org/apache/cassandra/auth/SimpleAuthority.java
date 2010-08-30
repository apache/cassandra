package org.apache.cassandra.auth;
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


import java.io.*;
import java.util.EnumSet;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;

public class SimpleAuthority implements IAuthority
{
    public final static String ACCESS_FILENAME_PROPERTY = "access.properties";

    @Override
    public EnumSet<Permission> authorize(AuthenticatedUser user, String keyspace)
    {
        String afilename = System.getProperty(ACCESS_FILENAME_PROPERTY);
        EnumSet<Permission> authorized = Permission.NONE;
        try
        {
            FileInputStream in = new FileInputStream(afilename);
            Properties props = new Properties();
            props.load(in);
            in.close();

            // structure:
            // given keyspace X, users A B and C can be authorized like this (separate their names with spaces):
            // X = A B C
            
            // note we keep the message here and for other authorization problems exactly the same to prevent attackers
            // from guessing what keyspaces are valid
            if (null == props.getProperty(keyspace))
                return authorized;

            for (String allow : props.getProperty(keyspace).split(","))
                if (allow.equals(user.username))
                    authorized = Permission.ALL;
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Authorization table file '%s' could not be opened: %s", afilename, e.getMessage()));
        }

        return authorized;
    }

    @Override
    public void validateConfiguration() throws ConfigurationException 
    {
        String afilename = System.getProperty(ACCESS_FILENAME_PROPERTY);
        if (afilename == null)
        {
            throw new ConfigurationException(String.format("When using %s, '%s' property must be defined.",
                                                           this.getClass().getCanonicalName(),
                                                           ACCESS_FILENAME_PROPERTY));	
        }
    }
}

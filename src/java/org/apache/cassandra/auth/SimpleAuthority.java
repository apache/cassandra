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
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;

public class SimpleAuthority implements IAuthority
{
    public final static String ACCESS_FILENAME_PROPERTY = "access.properties";
    // magical property for WRITE permissions to the keyspaces list
    public final static String KEYSPACES_WRITE_PROPERTY = "<modify-keyspaces>";

    @Override
    public EnumSet<Permission> authorize(AuthenticatedUser user, List<Object> resource)
    {
        if (resource.size() < 2 || !Resources.ROOT.equals(resource.get(0)) || !Resources.KEYSPACES.equals(resource.get(1)))
            // we only know how to handle keyspace authorization
            return Permission.NONE;

        String keyspace;
        EnumSet<Permission> authorized;
        if (resource.size() < 3)
        {
            // authorize the user for the keyspace list using the 'magical' keyspace,
            // but give them read access by default
            keyspace = KEYSPACES_WRITE_PROPERTY;
            authorized = EnumSet.of(Permission.READ);
        }
        else
        {
            // otherwise, authorize them for the actual keyspace
            keyspace = (String)resource.get(2);
            authorized = Permission.NONE;
        }

        String afilename = System.getProperty(ACCESS_FILENAME_PROPERTY);
        try
        {
            FileInputStream in = new FileInputStream(afilename);
            Properties props = new Properties();
            props.load(in);
            in.close();

            // structure:
            // given keyspace X, users A B and C can be authorized like this (separate their names with spaces):
            // X = A B C
            
            if (null == props.getProperty(keyspace))
                // no one is authorized
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

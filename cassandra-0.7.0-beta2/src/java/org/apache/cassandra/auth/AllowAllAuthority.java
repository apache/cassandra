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

import java.util.EnumSet;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.AuthorizationException;

public class AllowAllAuthority implements IAuthority
{
    @Override
    public EnumSet<Permission> authorize(AuthenticatedUser user, String keyspace)
    {
        return Permission.ALL;
    }

    @Override    
    public void validateConfiguration() throws ConfigurationException
    {
        // pass
    }
}

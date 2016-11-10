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
package org.apache.cassandra.auth;

import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.MigrationListener;

/**
 * MigrationListener implementation that cleans up permissions on dropped resources.
 */
public class AuthMigrationListener extends MigrationListener
{
    public void onDropKeyspace(String ksName)
    {
        DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.keyspace(ksName));
        DatabaseDescriptor.getAuthorizer().revokeAllOn(FunctionResource.keyspace(ksName));
    }

    public void onDropColumnFamily(String ksName, String cfName)
    {
        DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.table(ksName, cfName));
    }

    public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
    {
        DatabaseDescriptor.getAuthorizer()
                          .revokeAllOn(FunctionResource.function(ksName, functionName, argTypes));
    }

    public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
    {
        DatabaseDescriptor.getAuthorizer()
                          .revokeAllOn(FunctionResource.function(ksName, aggregateName, argTypes));
    }
}

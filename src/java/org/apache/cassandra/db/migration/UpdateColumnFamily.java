/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.CfDef;

public class UpdateColumnFamily extends Migration
{
    private final CfDef newState;

    public UpdateColumnFamily(CfDef newState) throws ConfigurationException
    {
        super(System.nanoTime());

        if (Schema.instance.getCFMetaData(newState.keyspace, newState.name) == null)
            throw new ConfigurationException(String.format("(ks=%s, cf=%s) cannot be updated because it doesn't exist.", newState.keyspace, newState.name));

        this.newState = newState;
    }

    protected Collection<RowMutation> applyImpl() throws ConfigurationException, IOException
    {
        return MigrationHelper.updateColumnFamily(newState, timestamp);
    }

    @Override
    public String toString()
    {
        return String.format("Update column family with %s", newState);
    }
}

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
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.RowMutation;

public class DropColumnFamily extends Migration
{
    private final String ksName;
    private final String cfName;
    
    public DropColumnFamily(String ksName, String cfName) throws ConfigurationException
    {
        super(System.nanoTime());

        KSMetaData ksm = Schema.instance.getTableDefinition(ksName);
        if (ksm == null)
            throw new ConfigurationException("Can't drop ColumnFamily: No such keyspace '" + ksName + "'.");
        else if (!ksm.cfMetaData().containsKey(cfName))
            throw new ConfigurationException(String.format("Can't drop ColumnFamily (ks=%s, cf=%s) : Not defined in that keyspace.", ksName, cfName));

        this.ksName = ksName;
        this.cfName = cfName;
    }

    protected Collection<RowMutation> applyImpl() throws ConfigurationException, IOException
    {
        return MigrationHelper.dropColumnFamily(ksName, cfName, timestamp);
    }

    @Override
    public String toString()
    {
        return String.format("Drop column family: %s.%s", ksName, cfName);
    }
}

package org.apache.cassandra.cache;
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

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;

import com.sun.jna.Memory;

public class SerializingCacheProvider implements IRowCacheProvider
{
    public SerializingCacheProvider() throws ConfigurationException
    {
        try
        {
            Memory.class.getName();
        }
        catch (NoClassDefFoundError e)
        {
            throw new ConfigurationException("Cannot initialize SerializationCache without JNA in the class path");
        }
    }

    public ICache<DecoratedKey, ColumnFamily> create(int capacity, String tableName, String cfName)
    {
        return new SerializingCache<DecoratedKey, ColumnFamily>(capacity, ColumnFamily.serializer(), tableName, cfName);
    }
}

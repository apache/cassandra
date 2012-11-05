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
package org.apache.cassandra.db.index.keys;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public class KeysIndex extends AbstractSimplePerColumnSecondaryIndex
{
    public void init(ColumnDefinition columnDef)
    {
        // Nothing specific
    }

    protected ByteBuffer makeIndexColumnName(ByteBuffer rowKey, IColumn column)
    {
        return rowKey;
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new KeysSearcher(baseCfs.indexManager, columns);
    }

    public void validateOptions() throws ConfigurationException
    {
        // no options used
    }

    protected AbstractType getExpressionComparator()
    {
        return baseCfs.getComparator();
    }
}

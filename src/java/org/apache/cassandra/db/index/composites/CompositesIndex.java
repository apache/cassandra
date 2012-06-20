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
package org.apache.cassandra.db.index.composites;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public class CompositesIndex extends AbstractSimplePerColumnSecondaryIndex
{
    public static final String PREFIX_SIZE_OPTION = "prefix_size";

    private CompositeType indexComparator;
    private int prefixSize;

    public void init(ColumnDefinition columnDef)
    {
        assert baseCfs.getComparator() instanceof CompositeType;
        CompositeType baseComparator = (CompositeType) baseCfs.getComparator();
        try
        {
            prefixSize = Integer.parseInt(columnDef.getIndexOptions().get(PREFIX_SIZE_OPTION));
        }
        catch (NumberFormatException e)
        {
            // Shouldn't happen since validateOptions must have been called
            throw new AssertionError(e);
        }

        indexComparator = (CompositeType)SecondaryIndex.getIndexComparator(baseCfs.metadata, columnDef);
    }

    protected ByteBuffer makeIndexColumnName(ByteBuffer rowKey, IColumn column)
    {
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(column.name());
        CompositeType.Builder builder = new CompositeType.Builder(indexComparator);
        builder.add(rowKey);
        for (int i = 0; i < Math.min(prefixSize, components.length); i++)
            builder.add(components[i]);
        return builder.build();
    }

    @Override
    public boolean indexes(ByteBuffer name)
    {
        ColumnDefinition columnDef = columnDefs.iterator().next();
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(name);
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return components.length > columnDef.componentIndex
            && comp.compare(components[columnDef.componentIndex], columnDef.name) == 0;
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new CompositesSearcher(baseCfs.indexManager, columns, prefixSize);
    }

    public void validateOptions() throws ConfigurationException
    {
        ColumnDefinition columnDef = columnDefs.iterator().next();
        String option = columnDef.getIndexOptions().get(PREFIX_SIZE_OPTION);

        if (option == null)
            throw new ConfigurationException("Missing option " + PREFIX_SIZE_OPTION);

        try
        {
            Integer.parseInt(option);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid non integer value for option %s (got '%s')", PREFIX_SIZE_OPTION, option));
        }
    }
}

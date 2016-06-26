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

package org.apache.cassandra.index.sasi;

import java.io.IOException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.sstable.format.*;

/**
 * KeyFetcher, fetches clustering and partition key from the sstable.
 *
 * Currently, clustering key is fetched from the data file of the sstable and partition key is
 * read from the index file. Reading from index file helps us to warm up key cache in this case.
 */
public class KeyFetcher
{
    private final SSTableReader sstable;

    public KeyFetcher(SSTableReader reader)
    {
        sstable = reader;
    }

    public ClusteringPrefix getClustering(Long offset)
    {
        try
        {
            Clustering clustering = sstable.clusteringAt(offset);

            // TODO: (ifesdjeen) use clustering, not prefix, or maybe start reading out here even,
            // alternative path would be to use order-preserving clustering hash in Token, although that'd
            // make everything heavier.
            if (clustering != null)
                return clustering.clustering();
            else
                return null;
        }
        catch (IOException e)
        {
            throw new FSReadError(new IOException("Failed to read clustering from " + sstable.descriptor, e), sstable.getFilename());
        }
    }

    public DecoratedKey getPartitionKey(Long offset)
    {
        try
        {
            return sstable.keyAt(offset);
        }
        catch (IOException e)
        {
            throw new FSReadError(new IOException("Failed to read key from " + sstable.descriptor, e), sstable.getFilename());
        }
    }

    public int hashCode()
    {
        return sstable.descriptor.hashCode();
    }

    public boolean equals(Object other)
    {
        return other instanceof KeyFetcher
               && sstable.descriptor.equals(((KeyFetcher) other).sstable.descriptor);
    }
}
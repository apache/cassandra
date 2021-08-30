/*
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
 */
package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;

public class TombstoneOverwhelmingException extends RejectException
{
    public TombstoneOverwhelmingException(int numTombstones, String query, TableMetadata metadata, DecoratedKey lastPartitionKey, ClusteringPrefix<?> lastClustering)
    {
        super(String.format("Scanned over %d tombstones during query '%s' (last scanned row token was %s and partion key was (%s)); query aborted",
                            numTombstones, query, lastPartitionKey.getToken(), makePKString(metadata, lastPartitionKey.getKey(), lastClustering)));
    }

    private static String makePKString(TableMetadata metadata, ByteBuffer partitionKey, ClusteringPrefix<?> clustering)
    {
        StringBuilder sb = new StringBuilder();

        if (clustering.size() > 0)
            sb.append("(");

        // TODO: We should probably make that a lot easier/transparent for partition keys
        AbstractType<?> pkType = metadata.partitionKeyType;
        if (pkType instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)pkType;
            ByteBuffer[] values = ct.split(partitionKey);
            for (int i = 0; i < values.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(ct.types.get(i).getString(values[i]));
            }
        }
        else
        {
            sb.append(pkType.getString(partitionKey));
        }

        if (clustering.size() > 0)
            sb.append(")");

        for (int i = 0; i < clustering.size(); i++)
            sb.append(", ").append(clustering.stringAt(i, metadata.comparator));

        return sb.toString();
    }
}

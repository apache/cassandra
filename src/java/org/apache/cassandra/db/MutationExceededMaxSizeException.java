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

package org.apache.cassandra.db;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.db.IMutation.MAX_MUTATION_SIZE;

public class MutationExceededMaxSizeException extends InvalidRequestException
{
    public static final int PARTITION_MESSAGE_LIMIT = 1024;

    public final long mutationSize;

    MutationExceededMaxSizeException(IMutation mutation, int serializationVersion, long totalSize)
    {
        super(prepareMessage(mutation, serializationVersion, totalSize));
        this.mutationSize = totalSize;
    }

    private static String prepareMessage(IMutation mutation, int version, long totalSize)
    {
        List<String> topPartitions = mutation.getPartitionUpdates().stream()
                                             .sorted((upd1, upd2) ->
                                                     Long.compare(PartitionUpdate.serializer.serializedSize(upd2, version),
                                                                  PartitionUpdate.serializer.serializedSize(upd1, version)))
                                             .map(upd -> String.format("%s.%s",
                                                                       upd.metadata().name,
                                                                       upd.metadata().partitionKeyType.getString(upd.partitionKey().getKey())))
                                             .collect(Collectors.toList());

        String topKeys = makeTopKeysString(topPartitions, PARTITION_MESSAGE_LIMIT);
        return String.format("Rejected an oversized mutation (%d/%d) for keyspace: %s. Top keys are: %s",
                             totalSize,
                             MAX_MUTATION_SIZE,
                             mutation.getKeyspaceName(),
                             topKeys);
    }

    @VisibleForTesting
    static String makeTopKeysString(List<String> keys, int maxLength) {
        Iterator<String> iterator = keys.listIterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext())
        {
            String key = iterator.next();

            if (stringBuilder.length() == 0)
            {
                stringBuilder.append(key); //ensures atleast one key is added
                iterator.remove();
            }
            else if (stringBuilder.length() + key.length() + 2 <= maxLength) // 2 for ", "
            {
                stringBuilder.append(", ").append(key);
                iterator.remove();
            }
            else
                break;
        }

        if (keys.size() > 0)
            stringBuilder.append(" and ").append(keys.size()).append(" more.");

        return stringBuilder.toString();
    }
}

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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.MessageIn;

public class RowDigestResolver extends AbstractRowResolver
{
    public RowDigestResolver(String keyspaceName, ByteBuffer key)
    {
        super(key, keyspaceName);
    }

    /**
     * Special case of resolve() so that CL.ONE reads never throw DigestMismatchException in the foreground
     */
    public Row getData()
    {
        for (MessageIn<ReadResponse> message : replies)
        {
            ReadResponse result = message.payload;
            if (!result.isDigestQuery())
                return result.row();
        }
        return null;
    }

    /*
     * This method handles two different scenarios:
     *
     * a) we're handling the initial read, of data from the closest replica + digests
     *    from the rest.  In this case we check the digests against each other,
     *    throw an exception if there is a mismatch, otherwise return the data row.
     *
     * b) we're checking additional digests that arrived after the minimum to handle
     *    the requested ConsistencyLevel, i.e. asynchronous read repair check
     */
    public Row resolve() throws DigestMismatchException
    {
        if (logger.isDebugEnabled())
            logger.debug("resolving {} responses", replies.size());

        long start = System.nanoTime();

        // validate digests against each other; throw immediately on mismatch.
        // also extract the data reply, if any.
        ColumnFamily data = null;
        ByteBuffer digest = null;

        for (MessageIn<ReadResponse> message : replies)
        {
            ReadResponse response = message.payload;

            ByteBuffer newDigest;
            if (response.isDigestQuery())
            {
                newDigest = response.digest();
            }
            else
            {
                // note that this allows for multiple data replies, post-CASSANDRA-5932
                data = response.row().cf;
                newDigest = ColumnFamily.digest(data);
            }

            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                throw new DigestMismatchException(key, digest, newDigest);
        }

        if (logger.isDebugEnabled())
            logger.debug("resolve: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return new Row(key, data);
    }

    public boolean isDataPresent()
    {
        for (MessageIn<ReadResponse> message : replies)
        {
            if (!message.payload.isDigestQuery())
                return true;
        }
        return false;
    }
}

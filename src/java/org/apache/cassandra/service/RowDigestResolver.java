/**
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.Message;

public class RowDigestResolver extends AbstractRowResolver
{
    public RowDigestResolver(String table, ByteBuffer key)
    {
        super(key, table);
    }

    /**
     * Special case of resolve() so that CL.ONE reads never throw DigestMismatchException in the foreground
     */
    public Row getData() throws IOException
    {
        for (Map.Entry<Message, ReadResponse> entry : replies.entrySet())
        {
            ReadResponse result = entry.getValue();
            if (!result.isDigestQuery())
                return result.row();
        }

        throw new AssertionError("getData should not be invoked when no data is present");
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
    public Row resolve() throws DigestMismatchException, IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("resolving " + replies.size() + " responses");

        long startTime = System.currentTimeMillis();

        // validate digests against each other; throw immediately on mismatch.
        // also extract the data reply, if any.
        ColumnFamily data = null;
        ByteBuffer digest = null;
        for (Map.Entry<Message, ReadResponse> entry : replies.entrySet())
        {
            ReadResponse response = entry.getValue();
            if (response.isDigestQuery())
            {
                if (digest == null)
                {
                    digest = response.digest();
                }
                else
                {
                    ByteBuffer digest2 = response.digest();
                    if (!digest.equals(digest2))
                        throw new DigestMismatchException(key, digest, digest2);
                }
            }
            else
            {
                data = response.row().cf;
            }
        }

		// Compare digest (only one, since we threw earlier if there were different replies)
        // with the data response. If there is a mismatch then throw an exception so that read repair can happen.
        //
        // It's important to note that we do not consider the possibility of multiple data responses --
        // that can only happen when we're doing the repair post-mismatch, and will be handled by RowRepairResolver.
        if (digest != null)
        {
            ByteBuffer digest2 = ColumnFamily.digest(data);
            if (!digest.equals(digest2))
                throw new DigestMismatchException(key, digest, digest2);
            if (logger.isDebugEnabled())
                logger.debug("digests verified");
        }

        if (logger.isDebugEnabled())
            logger.debug("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");
		return new Row(key, data);
	}

    public boolean isDataPresent()
	{
        for (ReadResponse result : replies.values())
        {
            if (!result.isDigestQuery())
                return true;
        }
        return false;
    }
}

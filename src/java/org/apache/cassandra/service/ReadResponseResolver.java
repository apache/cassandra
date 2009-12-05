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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.io.DataInputBuffer;
import java.net.InetAddress;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.log4j.Logger;

public class ReadResponseResolver implements IResponseResolver<Row>
{
	private static Logger logger_ = Logger.getLogger(ReadResponseResolver.class);
    private final String table;
    private final int responseCount;

    public ReadResponseResolver(String table, int responseCount)
    {
        assert 1 <= responseCount && responseCount <= DatabaseDescriptor.getReplicationFactor()
            : "invalid response count " + responseCount;

        this.responseCount = responseCount;
        this.table = table;
    }

    /*
      * This method for resolving read data should look at the timestamps of each
      * of the columns that are read and should pick up columns with the latest
      * timestamp. For those columns where the timestamp is not the latest a
      * repair request should be scheduled.
      *
      */
	public Row resolve(List<Message> responses) throws DigestMismatchException, IOException
    {
        long startTime = System.currentTimeMillis();
		List<Row> rows = new ArrayList<Row>();
		List<InetAddress> endPoints = new ArrayList<InetAddress>();
		String key = null;
		byte[] digest = new byte[0];
		boolean isDigestQuery = false;
        
        /*
		 * Populate the list of rows from each of the messages
		 * Check to see if there is a digest query. If a digest 
         * query exists then we need to compare the digest with 
         * the digest of the data that is received.
        */
        DataInputBuffer bufIn = new DataInputBuffer();
		for (Message response : responses)
		{					            
            byte[] body = response.getMessageBody();
            bufIn.reset(body, body.length);
            ReadResponse result = ReadResponse.serializer().deserialize(bufIn);
            if (result.isDigestQuery())
            {
                digest = result.digest();
                isDigestQuery = true;
            }
            else
            {
                rows.add(result.row());
                endPoints.add(response.getFrom());
                key = result.row().key;
            }
        }
		// If there was a digest query compare it with all the data digests 
		// If there is a mismatch then throw an exception so that read repair can happen.
        if (isDigestQuery)
        {
            for (Row row : rows)
            {
                if (!Arrays.equals(row.digest(), digest))
                {
                    /* Wrap the key as the context in this exception */
                    String s = String.format("Mismatch for key %s (%s vs %s)", row.key, FBUtilities.bytesToHex(row.digest()), FBUtilities.bytesToHex(digest));
                    throw new DigestMismatchException(s);
                }
            }
        }

        Row resolved = resolveSuperset(rows);

        // At this point we have the return row;
        // Now we need to calculate the difference so that we can schedule read repairs
        for (int i = 0; i < rows.size(); i++)
        {
            // since retRow is the resolved row it can be used as the super set
            ColumnFamily diffCf = rows.get(i).diff(resolved);
            if (diffCf == null) // no repair needs to happen
                continue;
            // create the row mutation message based on the diff and schedule a read repair
            RowMutation rowMutation = new RowMutation(table, key);
            rowMutation.add(diffCf);
            RowMutationMessage rowMutationMessage = new RowMutationMessage(rowMutation);
            ReadRepairManager.instance().schedule(endPoints.get(i), rowMutationMessage);
        }
        if (logger_.isDebugEnabled())
            logger_.debug("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");
		return resolved;
	}

    static Row resolveSuperset(List<Row> rows)
    {
        assert rows.size() > 0;
        Row resolved = null;
        for (Row row : rows)
        {
            if (row.cf != null)
            {
                resolved = new Row(row.key, row.cf.cloneMe());
                break;
            }
        }
        if (resolved == null)
            return rows.get(0);
        for (Row row : rows)
        {
            resolved = resolved.resolve(row);
        }
        return resolved;
    }

	public boolean isDataPresent(List<Message> responses)
	{
        if (responses.size() < responseCount)
            return false;

        boolean isDataPresent = false;
        for (Message response : responses)
        {
            byte[] body = response.getMessageBody();
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            try
            {
                ReadResponse result = ReadResponse.serializer().deserialize(bufIn);
                if (!result.isDigestQuery())
                {
                    isDataPresent = true;
                }
                bufIn.close();
            }
            catch (IOException ex)
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
        return isDataPresent;
    }
}

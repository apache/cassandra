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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import java.net.InetAddress;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.log4j.Logger;

/**
 * Turns ReadResponse messages into Row objects, resolving to the most recent
 * version and setting up read repairs as necessary.
 */
public class ReadResponseResolver implements IResponseResolver<Row>
{
	private static Logger logger_ = Logger.getLogger(ReadResponseResolver.class);
    private final String table;
    private final int responseCount;

    public ReadResponseResolver(String table, int responseCount)
    {
        assert 1 <= responseCount && responseCount <= DatabaseDescriptor.getReplicationFactor(table)
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
	public Row resolve(Collection<Message> responses) throws DigestMismatchException, IOException
    {
        long startTime = System.currentTimeMillis();
		List<ColumnFamily> versions = new ArrayList<ColumnFamily>(responses.size());
		List<InetAddress> endPoints = new ArrayList<InetAddress>(responses.size());
		String key = null;
		byte[] digest = new byte[0];
		boolean isDigestQuery = false;
        
        /*
		 * Populate the list of rows from each of the messages
		 * Check to see if there is a digest query. If a digest 
         * query exists then we need to compare the digest with 
         * the digest of the data that is received.
        */
		for (Message response : responses)
		{					            
            byte[] body = response.getMessageBody();
            ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
            ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
            if (result.isDigestQuery())
            {
                digest = result.digest();
                isDigestQuery = true;
            }
            else
            {
                versions.add(result.row().cf);
                endPoints.add(response.getFrom());
                key = result.row().key;
            }
        }
		// If there was a digest query compare it with all the data digests 
		// If there is a mismatch then throw an exception so that read repair can happen.
        if (isDigestQuery)
        {
            for (ColumnFamily cf : versions)
            {
                if (!Arrays.equals(ColumnFamily.digest(cf), digest))
                {
                    /* Wrap the key as the context in this exception */
                    String s = String.format("Mismatch for key %s (%s vs %s)", key, FBUtilities.bytesToHex(ColumnFamily.digest(cf)), FBUtilities.bytesToHex(digest));
                    throw new DigestMismatchException(s);
                }
            }
        }

        ColumnFamily resolved = resolveSuperset(versions);
        maybeScheduleRepairs(resolved, table, key, versions, endPoints);

        if (logger_.isDebugEnabled())
            logger_.debug("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");
		return new Row(key, resolved);
	}

    /**
     * For each row version, compare with resolved (the superset of all row versions);
     * if it is missing anything, send a mutation to the endpoint it come from.
     */
    public static void maybeScheduleRepairs(ColumnFamily resolved, String table, String key, List<ColumnFamily> versions, List<InetAddress> endPoints)
    {
        for (int i = 0; i < versions.size(); i++)
        {
            ColumnFamily diffCf = ColumnFamily.diff(versions.get(i), resolved);
            if (diffCf == null) // no repair needs to happen
                continue;

            // create and send the row mutation message based on the diff
            RowMutation rowMutation = new RowMutation(table, key);
            rowMutation.add(diffCf);
            RowMutationMessage rowMutationMessage = new RowMutationMessage(rowMutation);
            Message repairMessage;
            try
            {
                repairMessage = rowMutationMessage.makeRowMutationMessage(StorageService.Verb.READ_REPAIR);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            MessagingService.instance.sendOneWay(repairMessage, endPoints.get(i));
        }
    }

    static ColumnFamily resolveSuperset(List<ColumnFamily> versions)
    {
        assert versions.size() > 0;
        ColumnFamily resolved = null;
        for (ColumnFamily cf : versions)
        {
            if (cf != null)
            {
                resolved = cf.cloneMe();
                break;
            }
        }
        if (resolved == null)
            return null;
        for (ColumnFamily cf : versions)
        {
            resolved.resolve(cf);
        }
        return resolved;
    }

	public boolean isDataPresent(Collection<Message> responses)
	{
        if (responses.size() < responseCount)
            return false;

        boolean isDataPresent = false;
        for (Message response : responses)
        {
            byte[] body = response.getMessageBody();
            ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
            try
            {
                ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
                if (!result.isDigestQuery())
                {
                    isDataPresent = true;
                }
                bufIn.close();
            }
            catch (IOException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        return isDataPresent;
    }
}

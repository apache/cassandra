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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractCommutativeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * Turns ReadResponse messages into Row objects, resolving to the most recent
 * version and setting up read repairs as necessary.
 */
public class ReadResponseResolver implements IResponseResolver<Row>
{
	private static Logger logger_ = LoggerFactory.getLogger(ReadResponseResolver.class);
    private final String table;
    private final Map<Message, ReadResponse> results = new NonBlockingHashMap<Message, ReadResponse>();
    private DecoratedKey key;

    public ReadResponseResolver(String table, ByteBuffer key)
    {
        this.table = table;
        this.key = StorageService.getPartitioner().decorateKey(key);
    }

    /*
     * This method handles two different scenarios:
     *
     * 1) we're handling the initial read, of data from the closest replica + digests
     *    from the rest.  In this case we check the digests against each other,
     *    throw an exception if there is a mismatch, otherwise return the data row.
     *
     * 2) there was a mismatch on the initial read, so we redid the digest requests
     *    as full data reads.  In this case we need to compute the most recent version
     *    of each column, and send diffs to out-of-date replicas.
     */
    public Row resolve() throws DigestMismatchException, IOException
    {
        if (logger_.isDebugEnabled())
            logger_.debug("resolving " + results.size() + " responses");

        long startTime = System.currentTimeMillis();
		List<ColumnFamily> versions = new ArrayList<ColumnFamily>();
		List<InetAddress> endpoints = new ArrayList<InetAddress>();
		ByteBuffer digest = null;

        // validate digests against each other; throw immediately on mismatch.
        // also, collects data results into versions/endpoints lists.
        for (Map.Entry<Message, ReadResponse> entry : results.entrySet())
        {
            ReadResponse result = entry.getValue();
            Message message = entry.getKey();
            ByteBuffer resultDigest = result.isDigestQuery() ? result.digest() : ColumnFamily.digest(result.row().cf);
            if (digest == null)
                digest = resultDigest;
            else if (!digest.equals(resultDigest))
                throw new DigestMismatchException(key, digest, resultDigest);

            if (!result.isDigestQuery())
            {
                ColumnFamily cf = result.row().cf;
                InetAddress from = message.getFrom();

                if (cf != null)
                {
                    AbstractType defaultValidator = cf.metadata().getDefaultValidator();
                    if (!FBUtilities.getLocalAddress().equals(from) && defaultValidator.isCommutative())
                    {
                        cf = cf.cloneMe();
                        ((AbstractCommutativeType) defaultValidator).cleanContext(cf, FBUtilities.getLocalAddress());
                    }
                }
                versions.add(cf);
                endpoints.add(from);
            }
        }

        if (logger_.isDebugEnabled())
            logger_.debug("digests verified");

        ColumnFamily resolved;
        if (versions.size() > 1)
        {
            resolved = resolveSuperset(versions);
            if (logger_.isDebugEnabled())
                logger_.debug("versions merged");
            maybeScheduleRepairs(resolved, table, key, versions, endpoints);
        }
        else
        {
            resolved = versions.get(0);
        }

        if (logger_.isDebugEnabled())
            logger_.debug("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");
		return new Row(key, resolved);
	}

    /**
     * For each row version, compare with resolved (the superset of all row versions);
     * if it is missing anything, send a mutation to the endpoint it come from.
     */
    public static void maybeScheduleRepairs(ColumnFamily resolved, String table, DecoratedKey key, List<ColumnFamily> versions, List<InetAddress> endpoints)
    {
        for (int i = 0; i < versions.size(); i++)
        {
            ColumnFamily diffCf = ColumnFamily.diff(versions.get(i), resolved);
            if (diffCf == null) // no repair needs to happen
                continue;

            // create and send the row mutation message based on the diff
            RowMutation rowMutation = new RowMutation(table, key.key);

            AbstractType defaultValidator = diffCf.metadata().getDefaultValidator();
            if (defaultValidator.isCommutative())
                ((AbstractCommutativeType)defaultValidator).cleanContext(diffCf, endpoints.get(i));

            if (diffCf.getColumnsMap().isEmpty() && !diffCf.isMarkedForDelete())
                continue;

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
            MessagingService.instance().sendOneWay(repairMessage, endpoints.get(i));
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
                resolved = cf.cloneMeShallow();
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

    public void preprocess(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
        try
        {
            ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
            if (logger_.isDebugEnabled())
                logger_.debug("Preprocessed {} response", result.isDigestQuery() ? "digest" : "data");
            results.put(message, result);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /** hack so ConsistencyChecker doesn't have to serialize/deserialize an extra real Message */
    public void injectPreProcessed(Message message, ReadResponse result)
    {
        results.put(message, result);
    }

    public boolean isDataPresent()
	{
        for (ReadResponse result : results.values())
        {
            if (!result.isDigestQuery())
                return true;
        }
        return false;
    }

    public Iterable<Message> getMessages()
    {
        return results.keySet();
    }

    public int getMessageCount()
    {
        return results.size();
    }
}

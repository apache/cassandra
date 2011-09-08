package org.apache.cassandra.service;
/*
 * 
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
 * 
 */


import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public abstract class AbstractRowResolver implements IResponseResolver<Row>
{
    protected static Logger logger = LoggerFactory.getLogger(AbstractRowResolver.class);

    private static final Message FAKE_MESSAGE = new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.INTERNAL_RESPONSE, ArrayUtils.EMPTY_BYTE_ARRAY, -1);

    protected final String table;
    protected final ConcurrentMap<Message, ReadResponse> replies = new NonBlockingHashMap<Message, ReadResponse>();
    protected final DecoratedKey<?> key;

    public AbstractRowResolver(ByteBuffer key, String table)
    {
        this.key = StorageService.getPartitioner().decorateKey(key);
        this.table = table;
    }

    public void preprocess(Message message)
    {
        byte[] body = message.getMessageBody();
        FastByteArrayInputStream bufIn = new FastByteArrayInputStream(body);
        try
        {
            ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn), message.getVersion());
            if (logger.isDebugEnabled())
                logger.debug("Preprocessed {} response", result.isDigestQuery() ? "digest" : "data");
            replies.put(message, result);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /** hack so local reads don't force de/serialization of an extra real Message */
    public void injectPreProcessed(ReadResponse result)
    {
        assert replies.get(FAKE_MESSAGE) == null; // should only be one local reply
        replies.put(FAKE_MESSAGE, result);
    }

    public Iterable<Message> getMessages()
    {
        return replies.keySet();
    }

    public int getMaxLiveColumns()
    {
        throw new UnsupportedOperationException();
    }
}

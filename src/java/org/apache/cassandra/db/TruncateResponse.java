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

package org.apache.cassandra.db;

import java.io.*;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;


/**
 * This message is sent back the truncate operation and basically specifies if
 * the truncate succeeded.
 */
public class TruncateResponse
{
    private static TruncateResponseSerializer serializer_ = new TruncateResponseSerializer();

    public static TruncateResponseSerializer serializer()
    {
        return serializer_;
    }

    public final String keyspace;
    public final String columnFamily;
    public final boolean success;


    public static Message makeTruncateResponseMessage(Message original, TruncateResponse truncateResponseMessage)
            throws IOException
    {
    	FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        TruncateResponse.serializer().serialize(truncateResponseMessage, dos, original.getVersion());
        return original.getReply(FBUtilities.getBroadcastAddress(), bos.toByteArray(), original.getVersion());
    }

    public TruncateResponse(String keyspace, String columnFamily, boolean success) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.success = success;
	}

    public static class TruncateResponseSerializer implements IVersionedSerializer<TruncateResponse>
    {
        public void serialize(TruncateResponse tr, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(tr.keyspace);
            dos.writeUTF(tr.columnFamily);
            dos.writeBoolean(tr.success);
        }

        public TruncateResponse deserialize(DataInput dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            String columnFamily = dis.readUTF();
            boolean success = dis.readBoolean();
            return new TruncateResponse(keyspace, columnFamily, success);
        }

        public long serializedSize(TruncateResponse truncateResponse, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}

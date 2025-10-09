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
package org.apache.cassandra.exceptions;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.MD5Digest;

public class PreparedQueryNotFoundException extends RequestValidationException
{
    public final MD5Digest id;

    public PreparedQueryNotFoundException(MD5Digest id)
    {
        super(ExceptionCode.UNPREPARED, makeMsg(id));
        this.id = id;
    }

    private static String makeMsg(MD5Digest id)
    {
        return String.format("Prepared query with ID %s not found" +
                             " (either the query was not prepared on this host (maybe the host has been restarted?)" +
                             " or you have prepared too many queries and it has been evicted from the internal cache)",
                             id);
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        // MD5 digest is always 16 bytes
        out.write(id.bytes);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return id.bytes.length; // MD5 is always 16 bytes
    }

    static PreparedQueryNotFoundException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        byte[] bytes = new byte[16]; // MD5 is always 16 bytes
        in.readFully(bytes);
        MD5Digest id = MD5Digest.wrap(bytes);
        return new PreparedQueryNotFoundException(id);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.UNPREPARED;
    }
}

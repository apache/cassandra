/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Token<T> implements Comparable<Token<T>>, Serializable
{
    private static final long serialVersionUID = 1L;

    private static final TokenSerializer serializer = new TokenSerializer();
    public static TokenSerializer serializer()
    {
        return serializer;
    }

    public final T token;

    protected Token(T token)
    {
        this.token = token;
    }

    /**
     * This determines the comparison for node destination purposes.
     */
    abstract public int compareTo(Token<T> o);

    public String toString()
    {
        return token.toString();
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof Token)) {
            return false;
        }
        return token.equals(((Token)obj).token);
    }

    public int hashCode()
    {
        return token.hashCode();
    }

    public static abstract class TokenFactory<T>
    {
        public abstract ByteBuffer toByteArray(Token<T> token);
        public abstract Token<T> fromByteArray(ByteBuffer bytes);
        public abstract String toString(Token<T> token); // serialize as string, not necessarily human-readable
        public abstract Token<T> fromString(String string); // deserialize

        public abstract void validate(String token) throws ConfigurationException;
    }

    public static class TokenSerializer implements ISerializer<Token>
    {
        public void serialize(Token token, DataOutput dos) throws IOException
        {
            IPartitioner p = StorageService.getPartitioner();
            ByteBuffer b = p.getTokenFactory().toByteArray(token);
            ByteBufferUtil.writeWithLength(b, dos);
        }

        public Token deserialize(DataInput dis) throws IOException
        {
            IPartitioner p = StorageService.getPartitioner();
            int size = dis.readInt();
            byte[] bytes = new byte[size];
            dis.readFully(bytes);
            return p.getTokenFactory().fromByteArray(ByteBuffer.wrap(bytes));
        }

        public long serializedSize(Token object)
        {
            throw new UnsupportedOperationException();
        }
    }
}

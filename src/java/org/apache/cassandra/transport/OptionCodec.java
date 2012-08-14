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
package org.apache.cassandra.transport;

import java.lang.reflect.Array;
import java.util.EnumMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.utils.Pair;

public class OptionCodec<T extends Enum<T> & OptionCodec.Codecable<T>>
{
    public interface Codecable<T extends Enum<T>>
    {
        public int getId();

        public Object readValue(ChannelBuffer cb);
        public void writeValue(Object value, ChannelBuffer cb);
        public int serializedValueSize(Object obj);
    }

    private final Class<T> klass;
    private final T[] ids;

    @SuppressWarnings({"unchecked"})
    public OptionCodec(Class<T> klass)
    {
        this.klass = klass;

        T[] values = klass.getEnumConstants();
        int maxId = -1;
        for (T opt : values)
            maxId = Math.max(maxId, opt.getId());
        ids = (T[])Array.newInstance(klass, maxId + 1);
        for (T opt : values)
        {
            if (ids[opt.getId()] != null)
                throw new IllegalStateException(String.format("Duplicate option id %d", opt.getId()));
            ids[opt.getId()] = opt;
        }
    }

    private T fromId(int id)
    {
        T opt = ids[id];
        if (opt == null)
            throw new ProtocolException(String.format("Unknown option id %d", id));
        return opt;
    }

    public Map<T, Object> decode(ChannelBuffer body)
    {
        EnumMap<T, Object> options = new EnumMap<T, Object>(klass);
        int n = body.readUnsignedShort();
        for (int i = 0; i < n; i++)
        {
            T opt = fromId(body.readUnsignedShort());
            Object value = opt.readValue(body);
            if (options.containsKey(opt))
                throw new ProtocolException(String.format("Duplicate option %s in message", opt.name()));
            options.put(opt, value);
        }
        return options;
    }

    public ChannelBuffer encode(Map<T, Object> options)
    {
        int optLength = 2;
        for (Map.Entry<T, Object> entry : options.entrySet())
            optLength += 2 + entry.getKey().serializedValueSize(entry.getValue());
        ChannelBuffer cb = ChannelBuffers.buffer(optLength);
        cb.writeShort(options.size());
        for (Map.Entry<T, Object> entry : options.entrySet())
        {
            T opt = entry.getKey();
            cb.writeShort(opt.getId());
            opt.writeValue(entry.getValue(), cb);
        }
        return cb;
    }

    public Pair<T, Object> decodeOne(ChannelBuffer body)
    {
        T opt = fromId(body.readUnsignedShort());
        Object value = opt.readValue(body);
        return Pair.create(opt, value);
    }

    public ChannelBuffer encodeOne(Pair<T, Object> option)
    {
        T opt = option.left;
        Object obj = option.right;

        ChannelBuffer cb = ChannelBuffers.buffer(oneSerializedSize(option));

        cb.writeShort(opt.getId());
        opt.writeValue(obj, cb);
        return cb;
    }

    public int oneSerializedSize(Pair<T, Object> option)
    {
        T opt = option.left;
        Object obj = option.right;
        return 2 + opt.serializedValueSize(obj);
    }
}

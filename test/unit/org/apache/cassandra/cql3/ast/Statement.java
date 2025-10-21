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

package org.apache.cassandra.cql3.ast;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import accord.utils.Invariants;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface Statement extends Element
{
    enum Kind { SELECT, MUTATION, TXN }
    Kind kind();

    default Mutation asMutation()
    {
        if (kind() != Kind.MUTATION)
            throw Invariants.illegalState("Unable to return Mutation; kind=%s", kind());
        return (Mutation) this;
    }

    default Select asSelect()
    {
        if (kind() != Kind.SELECT)
            throw Invariants.illegalState("Unable to return Select; kind=%s", kind());
        return (Select) this;
    }

    default Txn asTxn()
    {
        if (kind() != Kind.TXN)
            throw Invariants.illegalState("Unable to return Txn; kind=%s", kind());
        return (Txn) this;
    }

    default Object[] binds()
    {
        return streamRecursive()
               .filter(e -> e instanceof Bind)
               .map(e -> ((Bind) e).value())
               .toArray(Object[]::new);
    }

    default ByteBuffer[] bindsEncoded()
    {
        return streamRecursive()
               .filter(e -> e instanceof Bind)
               .map(e -> ((Bind) e).valueEncoded())
               .toArray(ByteBuffer[]::new);
    }

    default String detailedToString()
    {
        Object[] binds = binds();
        return "CQL:\n" + toCQL() + "\nBinds:\n" + IntStream.range(0, binds.length)
                                                            .mapToObj(i -> i + " -> " + binds[i] == null ? "null" : binds[i].getClass().getCanonicalName() + "(" + normalize(binds[i]) + ")")
                                                            .collect(Collectors.joining("\n"));
    }

    Statement visit(Visitor v);

    default String debugCQL()
    {
        return visit(StandardVisitors.DEBUG).toCQL();
    }

    static boolean hasByteBuffer(Object value)
    {
        if (value == null)
            return false;
        if (value instanceof ByteBuffer)
            return true;
        else if (value instanceof Collection)
        {
            Collection<Object> collection = (Collection<Object>) value;
            return collection.stream().anyMatch(Statement::hasByteBuffer);
        }
        else if (value instanceof Map)
        {
            Map<Object, Object> map = (Map<Object, Object>) value;
            return map.entrySet().stream().anyMatch(e -> hasByteBuffer(e.getKey()) || hasByteBuffer(e.getValue()));
        }
        return false;
    }

    static String normalize(Object value)
    {
        if (value == null)
            return null;
        if (value instanceof ByteBuffer)
        {
            ByteBuffer bb = (ByteBuffer) value;
            if (bb.remaining() > 100)
            {
                bb = bb.duplicate();
                bb.limit(bb.position() + 100);
                return ByteBufferUtil.bytesToHex(bb) + "...";
            }
            return ByteBufferUtil.bytesToHex(bb);
        }
        else if (value instanceof Collection)
        {
            Collection<Object> collection = (Collection<Object>) value;
            if (hasByteBuffer(collection))
                return collection.stream().map(Statement::normalize).collect(Collectors.toList()).toString();
        }
        else if (value instanceof Map)
        {
            Map<Object, Object> map = (Map<Object, Object>) value;
            if (hasByteBuffer(map))
                return map.entrySet().stream()
                          .map(e -> normalize(e.getKey()) + " -> " + normalize(e.getValue()))
                          .collect(Collectors.joining(", ", "{", "}"));
        }
        return value.toString();
    }
}

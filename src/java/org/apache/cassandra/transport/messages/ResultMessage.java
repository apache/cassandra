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
package org.apache.cassandra.transport.messages;

import java.util.*;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.utils.MD5Digest;

public abstract class ResultMessage extends Message.Response
{
    public static final Message.Codec<ResultMessage> codec = new Message.Codec<ResultMessage>()
    {
        public ResultMessage decode(ChannelBuffer body)
        {
            Kind kind = Kind.fromId(body.readInt());
            return kind.subcodec.decode(body);
        }

        public ChannelBuffer encode(ResultMessage msg)
        {
            ChannelBuffer kcb = ChannelBuffers.buffer(4);
            kcb.writeInt(msg.kind.id);

            ChannelBuffer body = msg.encodeBody();
            return ChannelBuffers.wrappedBuffer(kcb, body);
        }
    };

    public enum Kind
    {
        VOID         (1, Void.subcodec),
        ROWS         (2, Rows.subcodec),
        SET_KEYSPACE (3, SetKeyspace.subcodec),
        PREPARED     (4, Prepared.subcodec),
        SCHEMA_CHANGE(5, SchemaChange.subcodec);

        public final int id;
        public final Message.Codec<ResultMessage> subcodec;

        private static final Kind[] ids;
        static
        {
            int maxId = -1;
            for (Kind k : Kind.values())
                maxId = Math.max(maxId, k.id);
            ids = new Kind[maxId + 1];
            for (Kind k : Kind.values())
            {
                if (ids[k.id] != null)
                    throw new IllegalStateException("Duplicate kind id");
                ids[k.id] = k;
            }
        }

        private Kind(int id, Message.Codec<ResultMessage> subcodec)
        {
            this.id = id;
            this.subcodec = subcodec;
        }

        public static Kind fromId(int id)
        {
            Kind k = ids[id];
            if (k == null)
                throw new ProtocolException(String.format("Unknown kind id %d in RESULT message", id));
            return k;
        }
    }

    public final Kind kind;

    protected ResultMessage(Kind kind)
    {
        super(Message.Type.RESULT);
        this.kind = kind;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    protected abstract ChannelBuffer encodeBody();

    public abstract CqlResult toThriftResult();

    public static class Void extends ResultMessage
    {
        // Even though we have no specific information here, don't make a
        // singleton since as each message it has in fact a streamid and connection.
        public Void()
        {
            super(Kind.VOID);
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                return new Void();
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof Void;
                return ChannelBuffers.EMPTY_BUFFER;
            }
        };

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            return new CqlResult(CqlResultType.VOID);
        }

        @Override
        public String toString()
        {
            return "EMPTY RESULT";
        }
    }

    public static class SetKeyspace extends ResultMessage
    {
        public final String keyspace;

        public SetKeyspace(String keyspace)
        {
            super(Kind.SET_KEYSPACE);
            this.keyspace = keyspace;
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                String keyspace = CBUtil.readString(body);
                return new SetKeyspace(keyspace);
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof SetKeyspace;
                return CBUtil.stringToCB(((SetKeyspace)msg).keyspace);
            }
        };

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            return new CqlResult(CqlResultType.VOID);
        }

        @Override
        public String toString()
        {
            return "RESULT set keyspace " + keyspace;
        }
    }

    public static class Rows extends ResultMessage
    {
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                return new Rows(ResultSet.codec.decode(body));
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof Rows;
                Rows rowMsg = (Rows)msg;
                return ResultSet.codec.encode(rowMsg.result);
            }
        };

        public final ResultSet result;

        public Rows(ResultSet result)
        {
            super(Kind.ROWS);
            this.result = result;
        }

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            return result.toThriftResult();
        }

        @Override
        public String toString()
        {
            return "ROWS " + result;
        }

    }

    public static class Prepared extends ResultMessage
    {
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
                return new Prepared(id, -1, ResultSet.Metadata.codec.decode(body));
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof Prepared;
                Prepared prepared = (Prepared)msg;
                assert prepared.statementId != null;
                return ChannelBuffers.wrappedBuffer(CBUtil.bytesToCB(prepared.statementId.bytes), ResultSet.Metadata.codec.encode(prepared.metadata));
            }
        };

        public final MD5Digest statementId;
        public final ResultSet.Metadata metadata;

        // statement id for CQL-over-thrift compatibility. The binary protocol ignore that.
        private final int thriftStatementId;

        public Prepared(MD5Digest statementId, List<ColumnSpecification> names)
        {
            this(statementId, -1, new ResultSet.Metadata(names));
        }

        public static Prepared forThrift(int statementId, List<ColumnSpecification> names)
        {
            return new Prepared(null, statementId, new ResultSet.Metadata(names));
        }

        private Prepared(MD5Digest statementId, int thriftStatementId, ResultSet.Metadata metadata)
        {
            super(Kind.PREPARED);
            this.statementId = statementId;
            this.thriftStatementId = thriftStatementId;
            this.metadata = metadata;
        }

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            throw new UnsupportedOperationException();
        }

        public CqlPreparedResult toThriftPreparedResult()
        {
            List<String> namesString = new ArrayList<String>(metadata.names.size());
            List<String> typesString = new ArrayList<String>(metadata.names.size());
            for (ColumnSpecification name : metadata.names)
            {
                namesString.add(name.toString());
                typesString.add(name.type.toString());
            }
            return new CqlPreparedResult(thriftStatementId, metadata.names.size()).setVariable_types(typesString).setVariable_names(namesString);
        }

        @Override
        public String toString()
        {
            return "RESULT PREPARED " + statementId + " " + metadata;
        }
    }

    public static class SchemaChange extends ResultMessage
    {
        public enum Change { CREATED, UPDATED, DROPPED }

        public final Change change;
        public final String keyspace;
        public final String columnFamily;

        public SchemaChange(Change change, String keyspace)
        {
            this(change, keyspace, "");
        }

        public SchemaChange(Change change, String keyspace, String columnFamily)
        {
            super(Kind.SCHEMA_CHANGE);
            this.change = change;
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                String cStr = CBUtil.readString(body);
                Change change = null;
                try
                {
                    change = Enum.valueOf(Change.class, cStr.toUpperCase());
                }
                catch (IllegalStateException e)
                {
                    throw new ProtocolException("Unknown Schema change action: " + cStr);
                }

                String keyspace = CBUtil.readString(body);
                String columnFamily = CBUtil.readString(body);
                return new SchemaChange(change, keyspace, columnFamily);

            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof SchemaChange;
                SchemaChange scm = (SchemaChange)msg;

                ChannelBuffer a = CBUtil.stringToCB(scm.change.toString());
                ChannelBuffer k = CBUtil.stringToCB(scm.keyspace);
                ChannelBuffer c = CBUtil.stringToCB(scm.columnFamily);
                return ChannelBuffers.wrappedBuffer(a, k, c);
            }
        };

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            return new CqlResult(CqlResultType.VOID);
        }

        @Override
        public String toString()
        {
            return "RESULT schema change " + change + " on " + keyspace + (columnFamily.isEmpty() ? "" : "." + columnFamily);
        }
    }
}

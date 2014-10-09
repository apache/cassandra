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

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.utils.MD5Digest;

public abstract class ResultMessage extends Message.Response
{
    public static final Message.Codec<ResultMessage> codec = new Message.Codec<ResultMessage>()
    {
        public ResultMessage decode(ByteBuf body, int version)
        {
            Kind kind = Kind.fromId(body.readInt());
            return kind.subcodec.decode(body, version);
        }

        public void encode(ResultMessage msg, ByteBuf dest, int version)
        {
            dest.writeInt(msg.kind.id);
            msg.kind.subcodec.encode(msg, dest, version);
        }

        public int encodedSize(ResultMessage msg, int version)
        {
            return 4 + msg.kind.subcodec.encodedSize(msg, version);
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
            public ResultMessage decode(ByteBuf body, int version)
            {
                return new Void();
            }

            public void encode(ResultMessage msg, ByteBuf dest, int version)
            {
                assert msg instanceof Void;
            }

            public int encodedSize(ResultMessage msg, int version)
            {
                return 0;
            }
        };

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
            public ResultMessage decode(ByteBuf body, int version)
            {
                String keyspace = CBUtil.readString(body);
                return new SetKeyspace(keyspace);
            }

            public void encode(ResultMessage msg, ByteBuf dest, int version)
            {
                assert msg instanceof SetKeyspace;
                CBUtil.writeString(((SetKeyspace)msg).keyspace, dest);
            }

            public int encodedSize(ResultMessage msg, int version)
            {
                assert msg instanceof SetKeyspace;
                return CBUtil.sizeOfString(((SetKeyspace)msg).keyspace);
            }
        };

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
            public ResultMessage decode(ByteBuf body, int version)
            {
                return new Rows(ResultSet.codec.decode(body, version));
            }

            public void encode(ResultMessage msg, ByteBuf dest, int version)
            {
                assert msg instanceof Rows;
                Rows rowMsg = (Rows)msg;
                ResultSet.codec.encode(rowMsg.result, dest, version);
            }

            public int encodedSize(ResultMessage msg, int version)
            {
                assert msg instanceof Rows;
                Rows rowMsg = (Rows)msg;
                return ResultSet.codec.encodedSize(rowMsg.result, version);
            }
        };

        public final ResultSet result;

        public Rows(ResultSet result)
        {
            super(Kind.ROWS);
            this.result = result;
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
            public ResultMessage decode(ByteBuf body, int version)
            {
                MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
                ResultSet.Metadata metadata = ResultSet.Metadata.codec.decode(body, version);

                ResultSet.Metadata resultMetadata = ResultSet.Metadata.EMPTY;
                if (version > 1)
                    resultMetadata = ResultSet.Metadata.codec.decode(body, version);

                return new Prepared(id, -1, metadata, resultMetadata);
            }

            public void encode(ResultMessage msg, ByteBuf dest, int version)
            {
                assert msg instanceof Prepared;
                Prepared prepared = (Prepared)msg;
                assert prepared.statementId != null;

                CBUtil.writeBytes(prepared.statementId.bytes, dest);
                ResultSet.Metadata.codec.encode(prepared.metadata, dest, version);
                if (version > 1)
                    ResultSet.Metadata.codec.encode(prepared.resultMetadata, dest, version);
            }

            public int encodedSize(ResultMessage msg, int version)
            {
                assert msg instanceof Prepared;
                Prepared prepared = (Prepared)msg;
                assert prepared.statementId != null;

                int size = 0;
                size += CBUtil.sizeOfBytes(prepared.statementId.bytes);
                size += ResultSet.Metadata.codec.encodedSize(prepared.metadata, version);
                if (version > 1)
                    size += ResultSet.Metadata.codec.encodedSize(prepared.resultMetadata, version);
                return size;
            }
        };

        public final MD5Digest statementId;

        /** Describes the variables to be bound in the prepared statement */
        public final ResultSet.Metadata metadata;

        /** Describes the results of executing this prepared statement */
        public final ResultSet.Metadata resultMetadata;

        // statement id for CQL-over-thrift compatibility. The binary protocol ignore that.
        private final int thriftStatementId;

        public Prepared(MD5Digest statementId, ParsedStatement.Prepared prepared)
        {
            this(statementId, -1, new ResultSet.Metadata(prepared.boundNames), extractResultMetadata(prepared.statement));
        }

        public static Prepared forThrift(int statementId, List<ColumnSpecification> names)
        {
            return new Prepared(null, statementId, new ResultSet.Metadata(names), ResultSet.Metadata.EMPTY);
        }

        private Prepared(MD5Digest statementId, int thriftStatementId, ResultSet.Metadata metadata, ResultSet.Metadata resultMetadata)
        {
            super(Kind.PREPARED);
            this.statementId = statementId;
            this.thriftStatementId = thriftStatementId;
            this.metadata = metadata;
            this.resultMetadata = resultMetadata;
        }

        private static ResultSet.Metadata extractResultMetadata(CQLStatement statement)
        {
            if (!(statement instanceof SelectStatement))
                return ResultSet.Metadata.EMPTY;

            return ((SelectStatement)statement).getResultMetadata();
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
            return "RESULT PREPARED " + statementId + " " + metadata + " (resultMetadata=" + resultMetadata + ")";
        }
    }

    public static class SchemaChange extends ResultMessage
    {
        public final Event.SchemaChange change;

        public SchemaChange(Event.SchemaChange change)
        {
            super(Kind.SCHEMA_CHANGE);
            this.change = change;
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ByteBuf body, int version)
            {
                return new SchemaChange(Event.SchemaChange.deserializeEvent(body, version));
            }

            public void encode(ResultMessage msg, ByteBuf dest, int version)
            {
                assert msg instanceof SchemaChange;
                SchemaChange scm = (SchemaChange)msg;
                scm.change.serializeEvent(dest, version);
            }

            public int encodedSize(ResultMessage msg, int version)
            {
                assert msg instanceof SchemaChange;
                SchemaChange scm = (SchemaChange)msg;
                return scm.change.eventSerializedSize(version);
            }
        };

        public CqlResult toThriftResult()
        {
            return new CqlResult(CqlResultType.VOID);
        }

        @Override
        public String toString()
        {
            return "RESULT schema change " + change;
        }
    }
}

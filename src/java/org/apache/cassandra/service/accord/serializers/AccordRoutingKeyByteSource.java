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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.service.accord.api.AccordRoutingKey.RoutingKeyKind.SENTINEL;

public class AccordRoutingKeyByteSource
{
    public static final ByteComparable.Version currentVersion = ByteComparable.Version.OSS50;

    private static final byte[] MIN_ORDER = { -1 };
    private static final byte[] TOKEN_ORDER = { 0 };
    private static final byte[] MAX_ORDER = { 1 };

    private static ByteSource minPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MIN_ORDER);
    }

    private static ByteSource tokenPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, TOKEN_ORDER);
    }

    private static ByteSource maxPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MAX_ORDER);
    }

    public static Serializer create(IPartitioner partitioner)
    {
        if (partitioner.isFixedLength())
            return new FixedLength(partitioner, currentVersion);
        return new VariableLength(partitioner, currentVersion);
    }

    public static FixedLength fixedLength(IPartitioner partitioner)
    {
        return new FixedLength(partitioner, currentVersion);
    }

    public static VariableLength variableLength(IPartitioner partitioner)
    {
        return new VariableLength(partitioner, currentVersion);
    }

    public static abstract class Serializer
    {
        protected final IPartitioner partitioner;
        protected final ByteComparable.Version version;
        protected final byte[] empty;

        protected Serializer(IPartitioner partitioner, ByteComparable.Version version, byte[] empty)
        {
            this.partitioner = partitioner;
            this.version = version;
            this.empty = empty;
        }

        public ByteSource minAsComparableBytes()
        {
            return ByteSource.withTerminator(ByteSource.TERMINATOR, minPrefix(), ByteSource.fixedLength(empty));
        }

        public ByteSource maxAsComparableBytes()
        {
            return ByteSource.withTerminator(ByteSource.TERMINATOR, maxPrefix(), ByteSource.fixedLength(empty));
        }

        public ByteSource asComparableBytes(Token token)
        {
            if (token.getPartitioner() != partitioner)
                throw new IllegalArgumentException("Attempted to use the wrong partitioner: given " + token.getPartitioner() + " but expected " + partitioner);
            return ByteSource.withTerminator(ByteSource.TERMINATOR, tokenPrefix(), token.asComparableBytes(version));
        }

        public <V> Token tokenFromComparableBytes(ValueAccessor<V> accessor, V data) throws IOException
        {
            return tokenFromComparableBytes(ByteSource.peekable(ByteSource.fixedLength(accessor, data)));
        }

        public Token tokenFromComparableBytes(ByteSource.Peekable bs) throws IOException
        {
            if (bs.peek() == ByteSource.TERMINATOR)
                throw new IOException("Unable to read prefix");
            ByteSource.Peekable component = progress(bs);

            var prefix = ByteSourceInverse.getOptionalSignedFixedLength(ByteArrayAccessor.instance, component, 1);
            if (prefix == null)
                throw new IOException("Unable to read prefix; prefix was null");
            if (!Arrays.equals(TOKEN_ORDER, prefix))
            {
                String match = Arrays.equals(MIN_ORDER, prefix) ? "min"
                                                                : Arrays.equals(MAX_ORDER, prefix) ? "max"
                                                                                                   : "unknown";
                throw new IOException("Attempt to read token from non-token value: was " + match);
            }
            component = ByteSourceInverse.nextComponentSource(bs);
            if (component == null)
                throw new IOException("Unable to read token; component was not found");
            return partitioner.getTokenFactory().fromComparableBytes(component, version);
        }

        public ByteSource asComparableBytes(AccordRoutingKey key)
        {
            var uuid = key.table().asUUID();
            ByteSource[] srcs = { LongType.instance.asComparableBytes(LongType.instance.decompose(uuid.getMostSignificantBits()), ByteComparable.Version.OSS50),
                                  LongType.instance.asComparableBytes(LongType.instance.decompose(uuid.getLeastSignificantBits()), ByteComparable.Version.OSS50),
                                  asComparableBytesNoTable(key) };
            return ByteSource.withTerminator(ByteSource.TERMINATOR, srcs);
        }

        public ByteSource asComparableBytesNoTable(AccordRoutingKey key)
        {
            return key.kindOfRoutingKey() == SENTINEL ? key.asSentinelKey().isMin ? minAsComparableBytes() : maxAsComparableBytes()
                                                      : asComparableBytes(key.token());
        }

        public <V> AccordRoutingKey fromComparableBytes(ValueAccessor<V> accessor, V data) throws IOException
        {
            return fromComparableBytes(accessor, data, version, partitioner);
        }

        public static <V> AccordRoutingKey fromComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version, @Nullable IPartitioner partitioner)
        {
            var bs = ByteSource.peekable(ByteSource.fixedLength(accessor, data));
            long[] uuidValues = new long[2];
            for (int i = 0; i < 2; i++)
            {
                if (bs.peek() == ByteSource.TERMINATOR)
                    throw new IllegalArgumentException("Unable to parse bytes");
                ByteSource.Peekable component = ByteSourceInverse.nextComponentSource(bs);
                long value = LongType.instance.compose(LongType.instance.fromComparableBytes(component, ByteComparable.Version.OSS50));
                uuidValues[i] = value;
            }
            TableId tableId = TableId.fromUUID(new UUID(uuidValues[0], uuidValues[1]));
            return fromComparableBytes(bs, tableId, version, partitioner);
        }

        public static <V> AccordRoutingKey fromComparableBytes(ValueAccessor<V> accessor, V data, TableId tableId, ByteComparable.Version version, @Nullable IPartitioner partitioner)
        {
            var bs = ByteSource.peekable(ByteSource.fixedLength(accessor, data));
            return fromComparableBytes(bs, tableId, version, partitioner);
        }

        public static <V> AccordRoutingKey fromComparableBytes(ByteSource.Peekable bs, TableId tableId, ByteComparable.Version version, @Nullable IPartitioner partitioner)
        {
            if (partitioner == null)
                partitioner = AccordKeyspace.partitioner(tableId);
            return fromComparableBytes(bs, tableId,
                                       (id, isMin) -> isMin ? AccordRoutingKey.SentinelKey.min(id) : AccordRoutingKey.SentinelKey.max(id),
                                       AccordRoutingKey.TokenKey::new,
                                       version, partitioner
            );
        }

        public static AccordRoutingKey fromComparableBytes(ByteSource.Peekable bs, TableId tableId,
                                                           BiFunction<TableId, Boolean, AccordRoutingKey> onSentinel,
                                                           BiFunction<TableId, Token, AccordRoutingKey> onToken,
                                                           ByteComparable.Version version, IPartitioner partitioner)
        {
            if (bs.peek() == ByteSource.TERMINATOR)
                throw new IllegalStateException("Unable to read prefix");
            ByteSource.Peekable component = progress(bs);

            var prefix = ByteSourceInverse.getOptionalSignedFixedLength(ByteArrayAccessor.instance, component, 1);
            if (prefix == null)
                throw new IllegalStateException("Unable to read prefix; prefix was null");
            if (Arrays.equals(TOKEN_ORDER, prefix))
            {
                component = ByteSourceInverse.nextComponentSource(bs);
                if (component == null)
                    throw new IllegalStateException("Unable to read token; component was not found");
                return onToken.apply(tableId, partitioner.getTokenFactory().fromComparableBytes(component, version));
            }
            if (Arrays.equals(MIN_ORDER, prefix))
                return onSentinel.apply(tableId, true);
            if (Arrays.equals(MAX_ORDER, prefix))
                return onSentinel.apply(tableId, false);
            throw new AssertionError("Unknown prefix");
        }

        private static ByteSource.Peekable progress(ByteSource.Peekable bs)
        {
            ByteSource.Peekable component = ByteSourceInverse.nextComponentSource(bs);
            if (component == null)
                throw new IllegalStateException("Unable to read prefix; component was not found");
            if (component.peek() == ByteSource.NEXT_COMPONENT)
            {
                // this came from (table, token_or_sentinel)
                component = ByteSourceInverse.nextComponentSource(bs);
                if (component == null)
                    throw new IllegalStateException("Unable to read prefix; component was not found");
            }
            return component;
        }

        public byte[] serialize(Token token)
        {
            return ByteSourceInverse.readBytes(asComparableBytes(token));
        }

        public byte[] serialize(AccordRoutingKey key)
        {
            return ByteSourceInverse.readBytes(asComparableBytes(key));
        }

        public byte[] serializeNoTable(AccordRoutingKey key)
        {
            return ByteSourceInverse.readBytes(asComparableBytesNoTable(key));
        }
    }

    public static class VariableLength extends Serializer
    {
        public VariableLength(IPartitioner partitioner, ByteComparable.Version version)
        {
            super(partitioner, version, ByteArrayUtil.EMPTY_BYTE_ARRAY);
        }
    }

    public static class FixedLength extends Serializer
    {
        public FixedLength(IPartitioner partitioner, ByteComparable.Version version)
        {
            super(partitioner, version, computeEmptyBytes(partitioner, version));
        }

        private static byte[] computeEmptyBytes(IPartitioner partitioner, ByteComparable.Version version)
        {
            if (!partitioner.isFixedLength())
                throw new IllegalArgumentException("Unable to use partitioner " + partitioner.getClass() + "; it is not fixed-length");

            int tokenSize = ByteSourceInverse.readBytes(partitioner.getMinimumToken().asComparableBytes(version)).length;
            return new byte[tokenSize];
        }

        public int valueSize()
        {
            return 4 + empty.length;
        }
    }
}

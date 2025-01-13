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
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.TriFunction;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class AccordRoutingKeyByteSource
{
    public static final ByteComparable.Version currentVersion = ByteComparable.Version.OSS50;

    // TODO (review): I'm not sure MIN_MIN has a use, maybe try and remove?
    private static final byte[] MIN_MIN_ORDER = { -2 };
    private static final byte MIN_MIN_ORDER_BYTE = -2;

    private static final byte[] MIN_MAX_ORDER = { -1 };
    private static final byte MIN_MAX_ORDER_BYTE = -1;


    private static final byte[] MIN_TOKEN_ORDER = { 0 };
    private static final byte MIN_TOKEN_ORDER_BYTE = 0;

    private static final byte[] TOKEN_ORDER = { 1 };
    private static final byte TOKEN_ORDER_BYTE = 1;

    private static final byte[] MAX_MIN_ORDER = { 2 };
    private static final byte MAX_MIN_ORDER_BYTE = 2;

    private static final byte[] MAX_MAX_ORDER = { 3 };
    private static final byte MAX_MAX_ORDER_BYTE = 3;

    private static ByteSource minMinPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MIN_MIN_ORDER);
    }

    private static ByteSource minMaxPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MIN_MAX_ORDER);
    }

    private static ByteSource minTokenPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MIN_TOKEN_ORDER);
    }

    private static ByteSource tokenPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, TOKEN_ORDER);
    }

    private static ByteSource maxMinPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MAX_MIN_ORDER);
    }

    private static ByteSource maxMaxPrefix()
    {
        return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MAX_MAX_ORDER);
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

        public ByteSource minMinAsComparableBytes()
        {
            return ByteSource.withTerminator(ByteSource.TERMINATOR, minMinPrefix(), ByteSource.fixedLength(empty));
        }

        public ByteSource minMaxAsComparableBytes()
        {
            return ByteSource.withTerminator(ByteSource.TERMINATOR, minMaxPrefix(), ByteSource.fixedLength(empty));
        }

        public ByteSource maxMinAsComparableBytes()
        {
            return ByteSource.withTerminator(ByteSource.TERMINATOR, maxMinPrefix(), ByteSource.fixedLength(empty));
        }

        public ByteSource maxMaxAsComparableBytes()
        {
            return ByteSource.withTerminator(ByteSource.TERMINATOR, maxMaxPrefix(), ByteSource.fixedLength(empty));
        }

        public ByteSource asComparableBytes(Token token, ByteSource prefix)
        {
            if (token.getPartitioner() != partitioner)
                throw new IllegalArgumentException("Attempted to use the wrong partitioner: given " + token.getPartitioner() + " but expected " + partitioner);
            return ByteSource.withTerminator(ByteSource.TERMINATOR, prefix, token.asComparableBytes(version));
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

            byte[] prefix = ByteSourceInverse.getOptionalSignedFixedLength(ByteArrayAccessor.instance, component, 1);
            if (prefix == null)
                throw new IOException("Unable to read prefix; prefix was null");

            switch (prefix[0])
            {
                case TOKEN_ORDER_BYTE:
                case MIN_TOKEN_ORDER_BYTE:
                    break;
                default:
                    String match = "unknown";
                    switch (prefix[0])
                    {
                        case MIN_MIN_ORDER_BYTE:
                            match = "minmin"; break;
                        case MIN_MAX_ORDER_BYTE:
                            match = "minmax"; break;
                        case MAX_MIN_ORDER_BYTE:
                            match = "maxmin"; break;
                        case MAX_MAX_ORDER_BYTE:
                            match = "maxmax"; break;
                    }
                    throw new IOException("Attempt to read token from non-token value: was " + match);
            }
            component = ByteSourceInverse.nextComponentSource(bs);
            if (component == null)
                throw new IOException("Unable to read token; component was not found");
            return partitioner.getTokenFactory().fromComparableBytes(component, version);
        }

        public ByteSource asComparableBytes(AccordRoutingKey key)
        {
            UUID uuid = key.table().asUUID();
            ByteSource[] srcs = { LongType.instance.asComparableBytes(LongType.instance.decompose(uuid.getMostSignificantBits()), ByteComparable.Version.OSS50),
                                  LongType.instance.asComparableBytes(LongType.instance.decompose(uuid.getLeastSignificantBits()), ByteComparable.Version.OSS50),
                                  asComparableBytesNoTable(key) };
            return ByteSource.withTerminator(ByteSource.TERMINATOR, srcs);
        }

        public ByteSource asComparableBytesNoTable(AccordRoutingKey key)
        {
            switch (key.kindOfRoutingKey())
            {
                case SENTINEL:
                    SentinelKey sentinelKey = key.asSentinelKey();
                    if (sentinelKey.isMinSentinel)
                    {
                        if (sentinelKey.isMinMinSentinel)
                            return minMinAsComparableBytes();
                        else
                            return minMaxAsComparableBytes();
                    }
                    else
                    {
                        if (sentinelKey.isMinMinSentinel)
                            return maxMinAsComparableBytes();
                        else
                            return maxMaxAsComparableBytes();
                    }
                case TOKEN:
                    return asComparableBytes(key.token(), tokenPrefix());
                case MIN_TOKEN:
                    return asComparableBytes(key.token(), minTokenPrefix());
                default:
                    throw new IllegalStateException("Unhandled routing key type " + key.kindOfRoutingKey());
            }
        }

        public <V> AccordRoutingKey fromComparableBytes(ValueAccessor<V> accessor, V data) throws IOException
        {
            return fromComparableBytes(accessor, data, version, partitioner);
        }

        public static <V> AccordRoutingKey fromComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version, @Nullable IPartitioner partitioner)
        {
            ByteSource.Peekable bs = ByteSource.peekable(ByteSource.fixedLength(accessor, data));
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
            ByteSource.Peekable bs = ByteSource.peekable(ByteSource.fixedLength(accessor, data));
            return fromComparableBytes(bs, tableId, version, partitioner);
        }

        public static <V> AccordRoutingKey fromComparableBytes(ByteSource.Peekable bs, TableId tableId, ByteComparable.Version version, @Nullable IPartitioner partitioner)
        {
            if (partitioner == null)
                partitioner = AccordKeyspace.partitioner(tableId);
            return fromComparableBytes(bs, tableId,
                                       SentinelKey::new,
                                       AccordRoutingKey.TokenKey::new,
                                       AccordRoutingKey.MinTokenKey::new,
                                       version, partitioner
            );
        }

        public static AccordRoutingKey fromComparableBytes(ByteSource.Peekable bs, TableId tableId,
                                                           TriFunction<TableId, Boolean, Boolean, AccordRoutingKey> onSentinel,
                                                           BiFunction<TableId, Token, AccordRoutingKey> onToken,
                                                           BiFunction<TableId, Token, AccordRoutingKey> onMinToken,
                                                           ByteComparable.Version version, IPartitioner partitioner)
        {
            if (bs.peek() == ByteSource.TERMINATOR)
                throw new IllegalStateException("Unable to read prefix");
            ByteSource.Peekable component = progress(bs);

            byte[] prefix = ByteSourceInverse.getOptionalSignedFixedLength(ByteArrayAccessor.instance, component, 1);
            if (prefix == null)
                throw new IllegalStateException("Unable to read prefix; prefix was null");
            switch (prefix[0])
            {
                case TOKEN_ORDER_BYTE:
                    component = ByteSourceInverse.nextComponentSource(bs);
                    if (component == null)
                        throw new IllegalStateException("Unable to read token; component was not found");
                    return onToken.apply(tableId, partitioner.getTokenFactory().fromComparableBytes(component, version));
                case MIN_TOKEN_ORDER_BYTE:
                    component = ByteSourceInverse.nextComponentSource(bs);
                    if (component == null)
                        throw new IllegalStateException("Unable to read token; component was not found");
                    return onMinToken.apply(tableId, partitioner.getTokenFactory().fromComparableBytes(component, version));
                case MIN_MIN_ORDER_BYTE:
                    return onSentinel.apply(tableId, true, true);
                case MIN_MAX_ORDER_BYTE:
                    return onSentinel.apply(tableId, true, false);
                case MAX_MIN_ORDER_BYTE:
                    return onSentinel.apply(tableId, false, true);
                case MAX_MAX_ORDER_BYTE:
                    return onSentinel.apply(tableId, false, false);
                default:
                    throw new AssertionError("Unknown prefix");
            }
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
            return ByteSourceInverse.readBytes(asComparableBytes(token, tokenPrefix()));
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

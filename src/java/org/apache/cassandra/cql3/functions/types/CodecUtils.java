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
package org.apache.cassandra.cql3.functions.types;

import java.nio.ByteBuffer;

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A set of utility methods to deal with type conversion and serialization.
 */
public final class CodecUtils
{

    private static final long MAX_CQL_LONG_VALUE = ((1L << 32) - 1);

    private static final long EPOCH_AS_CQL_LONG = (1L << 31);

    private CodecUtils()
    {
    }

    /**
     * Utility method that "packs" together a list of {@link ByteBuffer}s containing serialized
     * collection elements. Mainly intended for use with collection codecs when serializing
     * collections.
     *
     * @param buffers  the collection elements
     * @param elements the total number of elements
     * @param version  the protocol version to use
     * @return The serialized collection
     */
    public static ByteBuffer pack(ByteBuffer[] buffers, int elements, ProtocolVersion version)
    {
        int size = 0;
        for (ByteBuffer bb : buffers)
        {
            int elemSize = sizeOfValue(bb, version);
            size += elemSize;
        }
        ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize(version) + size);
        writeSize(result, elements, version);
        for (ByteBuffer bb : buffers) writeValue(result, bb, version);
        return (ByteBuffer) result.flip();
    }

    /**
     * Utility method that reads a size value. Mainly intended for collection codecs when
     * deserializing CQL collections.
     *
     * @param input   The ByteBuffer to read from.
     * @param version The protocol version to use.
     * @return The size value.
     */
    static int readSize(ByteBuffer input, ProtocolVersion version)
    {
        switch (version)
        {
            case V1:
            case V2:
                return getUnsignedShort(input);
            case V3:
            case V4:
            case V5:
            case V6:
                return input.getInt();
            default:
                throw new IllegalArgumentException(String.valueOf(version));
        }
    }

    /**
     * Utility method that writes a size value. Mainly intended for collection codecs when serializing
     * CQL collections.
     *
     * @param output  The ByteBuffer to write to.
     * @param size    The collection size.
     * @param version The protocol version to use.
     */
    private static void writeSize(ByteBuffer output, int size, ProtocolVersion version)
    {
        switch (version)
        {
            case V1:
            case V2:
                if (size > 65535)
                    throw new IllegalArgumentException(
                    String.format(
                    "Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements",
                    version.asInt(), size));
                output.putShort((short) size);
                break;
            case V3:
            case V4:
            case V5:
            case V6:
                output.putInt(size);
                break;
            default:
                throw new IllegalArgumentException(String.valueOf(version));
        }
    }

    /**
     * Utility method that reads a value. Mainly intended for collection codecs when deserializing CQL
     * collections.
     *
     * @param input   The ByteBuffer to read from.
     * @param version The protocol version to use.
     * @return The collection element.
     */
    public static ByteBuffer readValue(ByteBuffer input, ProtocolVersion version)
    {
        int size = readSize(input, version);
        return size < 0 ? null : readBytes(input, size);
    }

    /**
     * Utility method that writes a value. Mainly intended for collection codecs when deserializing
     * CQL collections.
     *
     * @param output  The ByteBuffer to write to.
     * @param value   The value to write.
     * @param version The protocol version to use.
     */
    public static void writeValue(ByteBuffer output, ByteBuffer value, ProtocolVersion version)
    {
        switch (version)
        {
            case V1:
            case V2:
                assert value != null;
                output.putShort((short) value.remaining());
                output.put(value.duplicate());
                break;
            case V3:
            case V4:
            case V5:
            case V6:
                if (value == null)
                {
                    output.putInt(-1);
                }
                else
                {
                    output.putInt(value.remaining());
                    output.put(value.duplicate());
                }
                break;
            default:
                throw new IllegalArgumentException(String.valueOf(version));
        }
    }

    /**
     * Read {@code length} bytes from {@code bb} into a new ByteBuffer.
     *
     * @param bb     The ByteBuffer to read.
     * @param length The number of bytes to read.
     * @return The read bytes.
     */
    public static ByteBuffer readBytes(ByteBuffer bb, int length)
    {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    /**
     * Converts an "unsigned" int read from a DATE value into a signed int.
     *
     * <p>The protocol encodes DATE values as <em>unsigned</em> ints with the Epoch in the middle of
     * the range (2^31). This method handles the conversion from an "unsigned" to a signed int.
     */
    static int fromUnsignedToSignedInt(int unsigned)
    {
        return unsigned + Integer.MIN_VALUE; // this relies on overflow for "negative" values
    }

    /**
     * Converts an int into an "unsigned" int suitable to be written as a DATE value.
     *
     * <p>The protocol encodes DATE values as <em>unsigned</em> ints with the Epoch in the middle of
     * the range (2^31). This method handles the conversion from a signed to an "unsigned" int.
     */
    static int fromSignedToUnsignedInt(int signed)
    {
        return signed - Integer.MIN_VALUE;
    }

    /**
     * Convert from a raw CQL long representing a numeric DATE literal to the number of days since the
     * Epoch. In CQL, numeric DATE literals are longs (unsigned integers actually) between 0 and 2^32
     * - 1, with the epoch in the middle; this method re-centers the epoch at 0.
     *
     * @param raw The CQL date value to convert.
     * @return The number of days since the Epoch corresponding to the given raw value.
     * @throws IllegalArgumentException if the value is out of range.
     */
    static int fromCqlDateToDaysSinceEpoch(long raw)
    {
        if (raw < 0 || raw > MAX_CQL_LONG_VALUE)
            throw new IllegalArgumentException(
            String.format(
            "Numeric literals for DATE must be between 0 and %d (got %d)",
            MAX_CQL_LONG_VALUE, raw));
        return (int) (raw - EPOCH_AS_CQL_LONG);
    }

    private static int sizeOfCollectionSize(ProtocolVersion version)
    {
        switch (version)
        {
            case V1:
            case V2:
                return 2;
            case V3:
            case V4:
            case V5:
            case V6:
                return 4;
            default:
                throw new IllegalArgumentException(String.valueOf(version));
        }
    }

    private static int sizeOfValue(ByteBuffer value, ProtocolVersion version)
    {
        switch (version)
        {
            case V1:
            case V2:
                int elemSize = value.remaining();
                if (elemSize > 65535)
                    throw new IllegalArgumentException(
                    String.format(
                    "Native protocol version %d supports only elements with size up to 65535 bytes - but element size is %d bytes",
                    version.asInt(), elemSize));
                return 2 + elemSize;
            case V3:
            case V4:
            case V5:
            case V6:
                return value == null ? 4 : 4 + value.remaining();
            default:
                throw new IllegalArgumentException(String.valueOf(version));
        }
    }

    private static int getUnsignedShort(ByteBuffer bb)
    {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }
}

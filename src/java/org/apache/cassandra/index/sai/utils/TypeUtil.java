/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.utils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class TypeUtil
{
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    /**
     * DecimalType / BigDecimal values are indexed by truncating their asComparableBytes representation to this size,
     * padding on the right with zero-value-bytes until this size is reached (if necessary).  This causes
     * false-positives that must be filtered in a separate step after hitting the index and reading the associated
     * (full) values.
     */
    public static final int DECIMAL_APPROXIMATION_BYTES = 24;

    private TypeUtil() {}

    /**
     * Returns <code>true</code> if given buffer would pass the {@link AbstractType#validate(ByteBuffer)}
     * check. False otherwise.
     */
    public static boolean isValid(ByteBuffer term, AbstractType<?> validator)
    {
        try
        {
            validator.validate(term);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    /**
     * Indicates if the type encoding supports rounding of the raw value.
     *
     * This is significant in range searches where we have to make all range
     * queries inclusive when searching the indexes in order to avoid excluding
     * rounded values. Excluded values are removed by post-filtering.
     */
    public static boolean supportsRounding(AbstractType<?> type)
    {
        return isBigInteger(type) || isBigDecimal(type);
    }

    /**
     * Returns the smaller of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer, AbstractType)} comparision.
     */
    public static ByteBuffer min(ByteBuffer a, ByteBuffer b, AbstractType<?> type)
    {
        return a == null ?  b : (b == null || compare(b, a, type) > 0) ? a : b;
    }

    /**
     * Returns the greater of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer, AbstractType)} comparision.
     */
    public static ByteBuffer max(ByteBuffer a, ByteBuffer b, AbstractType<?> type)
    {
        return a == null ?  b : (b == null || compare(b, a, type) < 0) ? a : b;
    }

    /**
     * Returns the lesser of two {@code ByteComparable} values, based on the result of {@link
     * ByteComparable#compare(ByteComparable, ByteComparable, ByteComparable.Version)} comparision.
     */
    public static ByteComparable min(ByteComparable a, ByteComparable b)
    {
        return a == null ?  b : (b == null || ByteComparable.compare(b, a, ByteComparable.Version.OSS41) > 0) ? a : b;
    }

    /**
     * Returns the greater of two {@code ByteComparable} values, based on the result of {@link
     * ByteComparable#compare(ByteComparable, ByteComparable, ByteComparable.Version)} comparision.
     */
    public static ByteComparable max(ByteComparable a, ByteComparable b)
    {
        return a == null ?  b : (b == null || ByteComparable.compare(b, a, ByteComparable.Version.OSS41) < 0) ? a : b;
    }

    /**
     * Returns the value length for the given {@link AbstractType}, selecting 16 for types
     * that officially use VARIABLE_LENGTH but are, in fact, of a fixed length.
     */
    public static int fixedSizeOf(AbstractType<?> type)
    {
        if (type.isValueLengthFixed())
            return type.valueLengthIfFixed();
        else if (isInetAddress(type))
            return 16;
        else if (isBigInteger(type))
            return 20;
        else if (type instanceof DecimalType)
            return DECIMAL_APPROXIMATION_BYTES;
        return 16;
    }

    public static AbstractType<?> cellValueType(Pair<ColumnMetadata, IndexTarget.Type> target)
    {
        AbstractType<?> type = target.left.type;
        if (isNonFrozenCollection(type))
        {
            CollectionType<?> collection = ((CollectionType<?>) type);
            switch (collection.kind)
            {
                case LIST:
                    return collection.valueComparator();
                case SET:
                    return collection.nameComparator();
                case MAP:
                    switch (target.right)
                    {
                        case KEYS:
                            return collection.nameComparator();
                        case VALUES:
                            return collection.valueComparator();
                        case KEYS_AND_VALUES:
                            return CompositeType.getInstance(collection.nameComparator(), collection.valueComparator());
                    }
            }
        }
        return type;
    }

    /**
     * Allows overriding the default getString method for {@link CompositeType}. It is
     * a requirement of the {@link ConcurrentRadixTree} that the keys are strings but
     * the getString method of {@link CompositeType} does not return a string that compares
     * in the same order as the underlying {@link ByteBuffer}. To get round this we convert
     * the {@link CompositeType} bytes to a hex string.
     */
    public static String getString(ByteBuffer value, AbstractType<?> type)
    {
        if (isComposite(type))
            return ByteBufferUtil.bytesToHex(value);
        return type.getString(value);
    }

    /**
     * The inverse of the above method. Overrides the fromString method on {@link CompositeType}
     * in order to convert the hex string to bytes.
     */
    public static ByteBuffer fromString(String value, AbstractType<?> type)
    {
        if (isComposite(type))
            return ByteBufferUtil.hexToBytes(value);
        return type.fromString(value);
    }

    public static ByteSource asComparableBytes(ByteBuffer value, AbstractType<?> type, ByteComparable.Version version)
    {
        if (type instanceof InetAddressType || type instanceof IntegerType || type instanceof DecimalType)
            return ByteSource.optionalFixedLength(ByteBufferAccessor.instance, value);
        return type.asComparableBytes(value, version);
    }

    /**
     * Fills a byte array with the comparable bytes for a type.
     * <p>
     * This method expects a {@code value} parameter generated by calling {@link #encode(ByteBuffer, AbstractType)}.
     * It is not generally safe to pass the output of other serialization methods to this method.  For instance, it is
     * not generally safe to pass the output of {@link AbstractType#decompose(Object)} as the {@code value} parameter
     * (there are certain types for which this is technically OK, but that doesn't hold for all types).
     *
     * @param value a value buffer returned by {@link #encode(ByteBuffer, AbstractType)}
     * @param type the type associated with the encoded {@code value} parameter
     * @param bytes this method's output
     */
    public static void toComparableBytes(ByteBuffer value, AbstractType<?> type, byte[] bytes)
    {
        if (isInetAddress(type))
            ByteBufferUtil.arrayCopy(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, 16);
        else if (isBigInteger(type))
            ByteBufferUtil.arrayCopy(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, 20);
        else if (type instanceof DecimalType)
            ByteBufferUtil.arrayCopy(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, DECIMAL_APPROXIMATION_BYTES);
        else
            ByteBufferUtil.toBytes(type.asComparableBytes(value, ByteComparable.Version.OSS41), bytes);
    }

    /**
     * Encode an external term from a memtable index or a compaction. The purpose of this is to
     * allow terms of particular types to be handled differently and not use the default
     * {@link ByteComparable} encoding.
     */
    public static ByteBuffer encode(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null)
            return null;

        if (isInetAddress(type))
            return encodeInetAddress(value);
        else if (isBigInteger(type))
            return encodeBigInteger(value);
        else if (type instanceof DecimalType)
            return encodeDecimal(value);
        return value;
    }

    /**
     * Compare two terms based on their type. This is used in place of {@link AbstractType#compare(ByteBuffer, ByteBuffer)}
     * so that the default comparison can be overridden for specific types.
     *
     * Note: This should be used for all term comparison
     */
    public static int compare(ByteBuffer b1, ByteBuffer b2, AbstractType<?> type)
    {
        if (isInetAddress(type))
            return compareInet(b1, b2);
        // BigInteger values, frozen types and composite types (map entries) use compareUnsigned to maintain
        // a consistent order between the in-memory index and the on-disk index.
        else if (isBigInteger(type) || isBigDecimal(type) || isCompositeOrFrozen(type))
            return FastByteOperations.compareUnsigned(b1, b2);

        return type.compare(b1, b2 );
    }

    /**
     * This is used for value comparison in post-filtering - {@link Expression#isSatisfiedBy(ByteBuffer)}.
     *
     * This allows types to decide whether they should be compared based on their encoded value or their
     * raw value. At present only {@link InetAddressType} values are compared by their encoded values to
     * allow for ipv4 -> ipv6 equivalency in searches.
     */
    public static int comparePostFilter(Expression.Value requestedValue, Expression.Value columnValue, AbstractType<?> type)
    {
        if (isInetAddress(type))
            return compareInet(requestedValue.encoded, columnValue.encoded);
        // Override comparisons for frozen collections and composite types (map entries)
        else if (isCompositeOrFrozen(type))
            return FastByteOperations.compareUnsigned(requestedValue.raw, columnValue.raw);

        return type.compare(requestedValue.raw, columnValue.raw);
    }

    public static Iterator<ByteBuffer> collectionIterator(AbstractType<?> validator,
                                                          ComplexColumnData cellData,
                                                          Pair<ColumnMetadata, IndexTarget.Type> target,
                                                          int nowInSecs)
    {
        if (cellData == null)
            return null;

        Stream<ByteBuffer> stream = StreamSupport.stream(cellData.spliterator(), false).filter(cell -> cell != null && cell.isLive(nowInSecs))
                                                 .map(cell -> cellValue(target, cell));

        if (isInetAddress(validator))
            stream = stream.sorted((c1, c2) -> compareInet(encodeInetAddress(c1), encodeInetAddress(c2)));

        return stream.iterator();
    }

    public static Comparator<ByteBuffer> comparator(AbstractType<?> type)
    {
        // Override the comparator for BigInteger, frozen collections and composite types
        if (isBigInteger(type) || isBigDecimal(type) || isCompositeOrFrozen(type))
            return FastByteOperations::compareUnsigned;

        return type;
    }

    private static ByteBuffer cellValue(Pair<ColumnMetadata, IndexTarget.Type> target, Cell cell)
    {
        if (target.left.type.isCollection() && target.left.type.isMultiCell())
        {
            switch (((CollectionType<?>) target.left.type).kind)
            {
                case LIST:
                    //TODO Is there any optimisation can be done here with cell values?
                    return cell.buffer();
                case SET:
                    return cell.path().get(0);
                case MAP:
                    switch (target.right)
                    {
                        case KEYS:
                            return cell.path().get(0);
                        case VALUES:
                            return cell.buffer();
                        case KEYS_AND_VALUES:
                            return CompositeType.build(ByteBufferAccessor.instance, cell.path().get(0), cell.buffer());
                    }
            }
        }
        return cell.buffer();
    }

    /**
     * Compares 2 InetAddress terms by ensuring that both addresses are represented as
     * ipv6 addresses.
     */
    private static int compareInet(ByteBuffer b1, ByteBuffer b2)
    {
        assert isIPv6(b1) && isIPv6(b2);

        return FastByteOperations.compareUnsigned(b1, b2);
    }

    private static boolean isIPv6(ByteBuffer address)
    {
        return address.remaining() == 16;
    }

    /**
     * Encode a {@link InetAddress} into a fixed width 16 byte encoded value.
     *
     * The encoded value is byte comparable and prefix compressible.
     *
     * The encoding is done by converting ipv4 addresses to their ipv6 equivalent.
     */
    private static ByteBuffer encodeInetAddress(ByteBuffer value)
    {
        if (value.remaining() == 4)
        {
            int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
            ByteBuffer mapped = ByteBuffer.allocate(16);
            System.arraycopy(IPV4_PREFIX, 0, mapped.array(), 0, IPV4_PREFIX.length);
            ByteBufferUtil.arrayCopy(value, position, mapped, IPV4_PREFIX.length, value.remaining());
            return mapped;
        }
        return value;
    }

    /**
     * Encode a {@link BigInteger} into a fixed width 20 byte encoded value.
     *
     * The encoded value is byte comparable and prefix compressible.
     *
     * The format of the encoding is:
     *
     *  The first 4 bytes contain the length of the {@link BigInteger} byte array
     *  with the top bit flipped for positive values.
     *
     *  The remaining 16 bytes contain the 16 most significant bytes of the
     *  {@link BigInteger} byte array.
     *
     *  For {@link BigInteger} values whose underlying byte array is less than
     *  16 bytes, the encoded value is sign extended.
     */
    public static ByteBuffer encodeBigInteger(ByteBuffer value)
    {
        int size = value.remaining();
        int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
        byte[] bytes = new byte[20];
        if (size < 16)
        {
            ByteBufferUtil.arrayCopy(value, position, bytes, bytes.length - size, size);
            if ((bytes[bytes.length - size] & 0x80) != 0)
                Arrays.fill(bytes, 4, bytes.length - size, (byte)0xff);
            else
                Arrays.fill(bytes, 4, bytes.length - size, (byte)0x00);
        }
        else
        {
            ByteBufferUtil.arrayCopy(value, position, bytes, 4, 16);
        }
        if ((bytes[4] & 0x80) != 0)
        {
            size = -size;
        }
        bytes[0] = (byte)(size >> 24 & 0xff);
        bytes[1] = (byte)(size >> 16 & 0xff);
        bytes[2] = (byte)(size >> 8 & 0xff);
        bytes[3] = (byte)(size & 0xff);
        bytes[0] ^= 0x80;
        return ByteBuffer.wrap(bytes);
    }

    /* Type comparison to get rid of ReversedType */

    /**
     * Returns <code>true</code> if values of the given {@link AbstractType} should be indexed as literals.
     */
    public static boolean isLiteral(AbstractType<?> type)
    {
        return isUTF8OrAscii(type) || isCompositeOrFrozen(type) || baseType(type) instanceof BooleanType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is UTF8 or Ascii
     */
    public static boolean isUTF8OrAscii(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof UTF8Type || type instanceof AsciiType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is a Composite(map entry) or frozen.
     */
    public static boolean isCompositeOrFrozen(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof CompositeType || isFrozen(type);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is frozen.
     */
    public static boolean isFrozen(AbstractType<?> type)
    {
        type = baseType(type);
        return !type.subTypes().isEmpty() && !type.isMultiCell();
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is a frozen collection.
     */
    public static boolean isFrozenCollection(AbstractType<?> type)
    {
        type = baseType(type);
        return type.isCollection() && !type.isMultiCell();
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is a non-frozen collection.
     */
    public static boolean isNonFrozenCollection(AbstractType<?> type)
    {
        type = baseType(type);
        return type.isCollection() && type.isMultiCell();
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is included in the types.
     */
    public static boolean isIn(AbstractType<?> type, Set<AbstractType<?>> types)
    {
        type = baseType(type);
        return types.contains(type);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link InetAddressType}
     */
    private static boolean isInetAddress(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof InetAddressType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link IntegerType}
     */
    private static boolean isBigInteger(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof IntegerType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link DecimalType}
     */
    private static boolean isBigDecimal(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof DecimalType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link CompositeType}
     */
    public static boolean isComposite(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof CompositeType;
    }

    /**
     * @return base type if given type is reversed, otherwise return itself
     */
    private static AbstractType<?> baseType(AbstractType<?> type)
    {
        return type.isReversed() ? ((ReversedType<?>) type).baseType : type;
    }

    public static ByteBuffer encodeDecimal(ByteBuffer value)
    {
        ByteSource bs = DecimalType.instance.asComparableBytes(value, ByteComparable.Version.OSS41);
        bs = ByteSource.cutOrRightPad(bs, DECIMAL_APPROXIMATION_BYTES, 0);
        return ByteBuffer.wrap(ByteSourceInverse.readBytes(bs, DECIMAL_APPROXIMATION_BYTES));
    }
}

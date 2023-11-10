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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * This class is a representation of an {@link AbstractType} as an indexable type. It is responsible for determining the
 * capabilities of the type and provides helper methods for handling term values associated with the type.
 */
public class IndexTermType
{
    private static final Set<AbstractType<?>> EQ_ONLY_TYPES = ImmutableSet.of(UTF8Type.instance,
                                                                              AsciiType.instance,
                                                                              BooleanType.instance,
                                                                              UUIDType.instance);

    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    /**
     * DecimalType / BigDecimal values are indexed by truncating their asComparableBytes representation to this size,
     * padding on the right with zero-value-bytes until this size is reached (if necessary).  This causes
     * false-positives that must be filtered in a separate step after hitting the index and reading the associated
     * (full) values.
     */
    private static final int DECIMAL_APPROXIMATION_BYTES = 24;
    private static final int BIG_INTEGER_APPROXIMATION_BYTES = 20;
    private static final int INET_ADDRESS_SIZE = 16;
    private static final int DEFAULT_FIXED_LENGTH = 16;

    private enum Capability
    {
        STRING,
        VECTOR,
        INET_ADDRESS,
        BIG_INTEGER,
        BIG_DECIMAL,
        LONG,
        BOOLEAN,
        LITERAL,
        REVERSED,
        FROZEN,
        COLLECTION,
        NON_FROZEN_COLLECTION,
        COMPOSITE,
        COMPOSITE_PARTITION
    }

    private final ColumnMetadata columnMetadata;
    private final IndexTarget.Type indexTargetType;
    private final AbstractType<?> indexType;
    private final List<IndexTermType> subTypes;
    private final AbstractType<?> vectorElementType;
    private final int vectorDimension;
    private final EnumSet<Capability> capabilities;

    /**
     * Create an {@link IndexTermType} from a {@link ColumnMetadata} and {@link IndexTarget.Type}.
     *
     * @param columnMetadata the {@link ColumnMetadata} for the column being indexed
     * @param partitionColumns the partition columns for the table this column belongs to. This is used for identifying
     *                         if the {@code columnMetadata} is a partition column and if it belongs to a composite
     *                         partition
     * @param indexTargetType the {@link IndexTarget.Type} for the index
     *
     * @return the {@link IndexTermType}
     */
    public static IndexTermType create(ColumnMetadata columnMetadata, List<ColumnMetadata> partitionColumns, IndexTarget.Type indexTargetType)
    {
        return new IndexTermType(columnMetadata, partitionColumns, indexTargetType);
    }

    private IndexTermType(ColumnMetadata columnMetadata, List<ColumnMetadata> partitionColumns, IndexTarget.Type indexTargetType)
    {
        this.columnMetadata = columnMetadata;
        this.indexTargetType = indexTargetType;
        this.capabilities = calculateCapabilities(columnMetadata, partitionColumns, indexTargetType);
        this.indexType = calculateIndexType(columnMetadata.type, capabilities, indexTargetType);
        if (indexType.subTypes().isEmpty())
        {
            this.subTypes = Collections.emptyList();
        }
        else
        {
            List<IndexTermType> subTypes = new ArrayList<>(indexType.subTypes().size());
            for (AbstractType<?> subType : indexType.subTypes())
                subTypes.add(new IndexTermType(columnMetadata.withNewType(subType), partitionColumns, indexTargetType));
            this.subTypes = Collections.unmodifiableList(subTypes);
        }
        if (isVector())
        {
            VectorType<?> vectorType = (VectorType<?>) indexType;
            vectorElementType = vectorType.elementType;
            vectorDimension = vectorType.dimension;
        }
        else
        {
            vectorElementType = null;
            vectorDimension = -1;
        }
    }

    /**
     * Returns {@code true} if the index type is a literal type and will use a literal index. This applies to
     * string types, frozen types, composite types and boolean type.
     */
    public boolean isLiteral()
    {
        return capabilities.contains(Capability.LITERAL);
    }

    /**
     * Returns {@code true} if the index type is a string type. This is used to determine if the type supports
     * analysis.
     */
    public boolean isString()
    {
        return capabilities.contains(Capability.STRING);
    }

    /**
     * Returns {@code true} if the index type is a vector type. Note: being a vector type does not mean that the type
     * is valid for indexing in that we don't check the element type and dimension constraints here.
     */
    public boolean isVector()
    {
        return capabilities.contains(Capability.VECTOR);
    }

    /**
     * Returns {@code true} if the index type is reversed. This is only the case (currently) for clustering keys with
     * descending ordering.
     */
    public boolean isReversed()
    {
        return capabilities.contains(Capability.REVERSED);
    }

    /**
     * Returns {@code true} if the index type is frozen, e.g. the type is wrapped with {@code frozen<type>}.
     */
    public boolean isFrozen()
    {
        return capabilities.contains(Capability.FROZEN);
    }

    /**
     * Returns {@code true} if the index type is a non-frozen collection
     */
    public boolean isNonFrozenCollection()
    {
        return capabilities.contains(Capability.NON_FROZEN_COLLECTION);
    }

    /**
     * Returns {@code true} if the index type is a frozen collection. This is the inverse of a non-frozen collection
     * but this method is here for clarity.
     */
    public boolean isFrozenCollection()
    {
        return capabilities.contains(Capability.COLLECTION) && capabilities.contains(Capability.FROZEN);
    }

    /**
     * Returns {@code true} if the index type is a composite type, e.g. it has the form {@code Composite<typea, typeb>}
     */
    public boolean isComposite()
    {
        return capabilities.contains(Capability.COMPOSITE);
    }

    /**
     * Returns {@code true} if the {@link RowFilter.Expression} passed is backed by a non-frozen collection and the
     * {@code Operator} is one that cannot be merged together.
     */
    public boolean isMultiExpression(RowFilter.Expression expression)
    {
        boolean multiExpression = false;
        switch (expression.operator())
        {
            case EQ:
                multiExpression = isNonFrozenCollection();
                break;
            case CONTAINS:
            case CONTAINS_KEY:
                multiExpression = true;
                break;
        }
        return multiExpression;
    }

    /**
     * Returns <code>true</code> if given buffer would pass the {@link AbstractType#validate(ByteBuffer)}
     * check. False otherwise.
     */
    public boolean isValid(ByteBuffer term)
    {
        try
        {
            indexType.validate(term);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    public AbstractType<?> indexType()
    {
        return indexType;
    }

    public Collection<IndexTermType> subTypes()
    {
        return subTypes;
    }

    public CQL3Type asCQL3Type()
    {
        return indexType.asCQL3Type();
    }

    public ColumnMetadata columnMetadata()
    {
        return columnMetadata;
    }

    public String columnName()
    {
        return columnMetadata.name.toString();
    }

    public AbstractType<?> vectorElementType()
    {
        assert isVector();

        return vectorElementType;
    }

    public int vectorDimension()
    {
        assert isVector();

        return vectorDimension;
    }

    public boolean dependsOn(ColumnMetadata columnMetadata)
    {
        return this.columnMetadata.compareTo(columnMetadata) == 0;
    }

    /**
     * Indicates if the type encoding supports rounding of the raw value.
     * <p>
     * This is significant in range searches where we have to make all range
     * queries inclusive when searching the indexes in order to avoid excluding
     * rounded values. Excluded values are removed by post-filtering.
     */
    public boolean supportsRounding()
    {
        return isBigInteger() || isBigDecimal();
    }

    /**
     * Returns the value length for the given {@link AbstractType}, selecting 16 for types
     * that officially use VARIABLE_LENGTH but are, in fact, of a fixed length.
     */
    public int fixedSizeOf()
    {
        if (indexType.isValueLengthFixed())
            return indexType.valueLengthIfFixed();
        else if (isInetAddress())
            return INET_ADDRESS_SIZE;
        else if (isBigInteger())
            return BIG_INTEGER_APPROXIMATION_BYTES;
        else if (isBigDecimal())
            return DECIMAL_APPROXIMATION_BYTES;
        return DEFAULT_FIXED_LENGTH;
    }

    /**
     * Allows overriding the default getString method for {@link CompositeType}. It is
     * a requirement of the {@link ConcurrentRadixTree} that the keys are strings but
     * the getString method of {@link CompositeType} does not return a string that compares
     * in the same order as the underlying {@link ByteBuffer}. To get round this we convert
     * the {@link CompositeType} bytes to a hex string.
     */
    public String asString(ByteBuffer value)
    {
        if (isComposite())
            return ByteBufferUtil.bytesToHex(value);
        return indexType.getString(value);
    }

    /**
     * The inverse of the above method. Overrides the fromString method on {@link CompositeType}
     * in order to convert the hex string to bytes.
     */
    public ByteBuffer fromString(String value)
    {
        if (isComposite())
            return ByteBufferUtil.hexToBytes(value);
        return indexType.fromString(value);
    }

    /**
     * Returns the cell value from the {@link DecoratedKey} or {@link Row} for the {@link IndexTermType} based on the
     * kind of column this {@link IndexTermType} is based on.
     *
     * @param key the {@link DecoratedKey} of the row
     * @param row the {@link Row} containing the non-partition column data
     * @param nowInSecs the time that the index write operation started
     *
     * @return a {@link ByteBuffer} containing the cell value
     */
    public ByteBuffer valueOf(DecoratedKey key, Row row, long nowInSecs)
    {
        if (row == null)
            return null;

        switch (columnMetadata.kind)
        {
            case PARTITION_KEY:
                return isCompositePartition() ? CompositeType.extractComponent(key.getKey(), columnMetadata.position())
                                              : key.getKey();
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                return row.isStatic() ? null : row.clustering().bufferAt(columnMetadata.position());

            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                Cell<?> cell = row.getCell(columnMetadata);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.buffer();

            default:
                return null;
        }
    }

    /**
     * Returns a value iterator for collection type {@link IndexTermType}s.
     *
     * @param row the {@link Row} containing the column data
     * @param nowInSecs the time that the index write operation started
     *
     * @return an {@link Iterator} of the collection values
     */
    public Iterator<ByteBuffer> valuesOf(Row row, long nowInSecs)
    {
        if (row == null)
            return null;

        switch (columnMetadata.kind)
        {
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                return collectionIterator(row.getComplexColumnData(columnMetadata), nowInSecs);

            default:
                return null;
        }
    }

    public Comparator<ByteBuffer> comparator()
    {
        // Override the comparator for BigInteger, frozen collections and composite types
        if (isBigInteger() || isBigDecimal() || isComposite() || isFrozen())
            return FastByteOperations::compareUnsigned;

        return indexType;
    }

    /**
     * Compare two terms based on their type. This is used in place of {@link AbstractType#compare(ByteBuffer, ByteBuffer)}
     * so that the default comparison can be overridden for specific types.
     * <p>
     * Note: This should be used for all term comparison
     */
    public int compare(ByteBuffer b1, ByteBuffer b2)
    {
        if (isInetAddress())
            return compareInet(b1, b2);
            // BigInteger values, frozen types and composite types (map entries) use compareUnsigned to maintain
            // a consistent order between the in-memory index and the on-disk index.
        else if (isBigInteger() || isBigDecimal() || isComposite() || isFrozen())
            return FastByteOperations.compareUnsigned(b1, b2);

        return indexType.compare(b1, b2 );
    }

    /**
     * Returns the smaller of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer)} comparision.
     */
    public ByteBuffer min(ByteBuffer a, ByteBuffer b)
    {
        return a == null ? b : (b == null || compare(b, a) > 0) ? a : b;
    }

    /**
     * Returns the greater of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer)} comparision.
     */
    public ByteBuffer max(ByteBuffer a, ByteBuffer b)
    {
        return a == null ? b : (b == null || compare(b, a) < 0) ? a : b;
    }

    /**
     * This is used for value comparison in post-filtering - {@link Expression#isSatisfiedBy(ByteBuffer)}.
     * <p>
     * This allows types to decide whether they should be compared based on their encoded value or their
     * raw value. At present only {@link InetAddressType} values are compared by their encoded values to
     * allow for ipv4 -> ipv6 equivalency in searches.
     */
    public int comparePostFilter(Expression.Value requestedValue, Expression.Value columnValue)
    {
        if (isInetAddress())
            return compareInet(requestedValue.encoded, columnValue.encoded);
            // Override comparisons for frozen collections and composite types (map entries)
        else if (isComposite() || isFrozen())
            return FastByteOperations.compareUnsigned(requestedValue.raw, columnValue.raw);

        return indexType.compare(requestedValue.raw, columnValue.raw);
    }

    /**
     * Fills a byte array with the comparable bytes for a type.
     * <p>
     * This method expects a {@code value} parameter generated by calling {@link #asIndexBytes(ByteBuffer)}.
     * It is not generally safe to pass the output of other serialization methods to this method.  For instance, it is
     * not generally safe to pass the output of {@link AbstractType#decompose(Object)} as the {@code value} parameter
     * (there are certain types for which this is technically OK, but that doesn't hold for all types).
     *
     * @param value a value buffer returned by {@link #asIndexBytes(ByteBuffer)}
     * @param bytes this method's output
     */
    public void toComparableBytes(ByteBuffer value, byte[] bytes)
    {
        if (isInetAddress())
            ByteBufferUtil.copyBytes(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, INET_ADDRESS_SIZE);
        else if (isBigInteger())
            ByteBufferUtil.copyBytes(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, BIG_INTEGER_APPROXIMATION_BYTES);
        else if (isBigDecimal())
            ByteBufferUtil.copyBytes(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, DECIMAL_APPROXIMATION_BYTES);
        else
            ByteSourceInverse.copyBytes(asComparableBytes(value, ByteComparable.Version.OSS50), bytes);
    }

    public ByteSource asComparableBytes(ByteBuffer value, ByteComparable.Version version)
    {
        if (isInetAddress() || isBigInteger() || isBigDecimal())
            return ByteSource.optionalFixedLength(ByteBufferAccessor.instance, value);
            // The LongType.asComparableBytes uses variableLengthInteger which doesn't play well with
            // the balanced tree because it is expecting fixed length data. So for SAI we use a optionalSignedFixedLengthNumber
            // to keep all comparable values the same length
        else if (isLong())
            return ByteSource.optionalSignedFixedLengthNumber(ByteBufferAccessor.instance, value);
        return indexType.asComparableBytes(value, version);
    }

    /**
     * Translates the external value of specific types into a format used by the index.
     */
    public ByteBuffer asIndexBytes(ByteBuffer value)
    {
        if (value == null)
            return null;

        if (isInetAddress())
            return encodeInetAddress(value);
        else if (isBigInteger())
            return encodeBigInteger(value);
        else if (isBigDecimal())
            return encodeDecimal(value);
        return value;
    }

    public float[] decomposeVector(ByteBuffer byteBuffer)
    {
        assert isVector();
        return ((VectorType<?>) indexType).composeAsFloat(byteBuffer);
    }

    public boolean supports(Operator operator)
    {
        if (operator == Operator.LIKE ||
            operator == Operator.LIKE_CONTAINS ||
            operator == Operator.LIKE_PREFIX ||
            operator == Operator.LIKE_MATCHES ||
            operator == Operator.LIKE_SUFFIX) return false;

        // ANN is only supported against vectors, and vector indexes only support ANN
        if (operator == Operator.ANN)
            return isVector();

        Expression.IndexOperator indexOperator = Expression.IndexOperator.valueOf(operator);

        if (isNonFrozenCollection())
        {
            if (indexTargetType == IndexTarget.Type.KEYS) return indexOperator == Expression.IndexOperator.CONTAINS_KEY;
            if (indexTargetType == IndexTarget.Type.VALUES) return indexOperator == Expression.IndexOperator.CONTAINS_VALUE;
            return indexTargetType == IndexTarget.Type.KEYS_AND_VALUES && indexOperator == Expression.IndexOperator.EQ;
        }

        if (indexTargetType == IndexTarget.Type.FULL)
            return indexOperator == Expression.IndexOperator.EQ;

        if (indexOperator != Expression.IndexOperator.EQ && EQ_ONLY_TYPES.contains(indexType)) return false;

        // RANGE only applicable to non-literal indexes
        return (indexOperator != null) && !(isLiteral() && indexOperator == Expression.IndexOperator.RANGE);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", columnMetadata)
                          .add("type", indexType)
                          .add("indexType", indexTargetType)
                          .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexTermType))
            return false;

        IndexTermType other = (IndexTermType) obj;

        return Objects.equals(columnMetadata, other.columnMetadata) && (indexTargetType == other.indexTargetType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnMetadata, indexTargetType);
    }

    private EnumSet<Capability> calculateCapabilities(ColumnMetadata columnMetadata, List<ColumnMetadata> partitionKeyColumns, IndexTarget.Type indexTargetType)
    {
        EnumSet<Capability> capabilities = EnumSet.noneOf(Capability.class);

        if (partitionKeyColumns.contains(columnMetadata) && partitionKeyColumns.size() > 1)
            capabilities.add(Capability.COMPOSITE_PARTITION);

        AbstractType<?> type = columnMetadata.type;

        if (type.isReversed())
            capabilities.add(Capability.REVERSED);

        AbstractType<?> baseType = type.unwrap();

        if (baseType.isCollection())
            capabilities.add(Capability.COLLECTION);

        if (baseType.isCollection() && baseType.isMultiCell())
            capabilities.add(Capability.NON_FROZEN_COLLECTION);

        if (!baseType.subTypes().isEmpty() && !baseType.isMultiCell())
            capabilities.add(Capability.FROZEN);

        AbstractType<?> indexType = calculateIndexType(baseType, capabilities, indexTargetType);

        if (indexType instanceof CompositeType)
            capabilities.add(Capability.COMPOSITE);
        else if (!indexType.subTypes().isEmpty() && !indexType.isMultiCell())
            capabilities.add(Capability.FROZEN);

        if (indexType instanceof StringType)
            capabilities.add(Capability.STRING);

        if (indexType instanceof BooleanType)
            capabilities.add(Capability.BOOLEAN);

        if (capabilities.contains(Capability.STRING) ||
            capabilities.contains(Capability.BOOLEAN) ||
            capabilities.contains(Capability.FROZEN) ||
            capabilities.contains(Capability.COMPOSITE))
            capabilities.add(Capability.LITERAL);

        if (indexType instanceof VectorType<?>)
            capabilities.add(Capability.VECTOR);

        if (indexType instanceof InetAddressType)
            capabilities.add(Capability.INET_ADDRESS);

        if (indexType instanceof IntegerType)
            capabilities.add(Capability.BIG_INTEGER);

        if (indexType instanceof DecimalType)
            capabilities.add(Capability.BIG_DECIMAL);

        if (indexType instanceof LongType)
            capabilities.add(Capability.LONG);

        return capabilities;
    }

    private AbstractType<?> calculateIndexType(AbstractType<?> baseType, EnumSet<Capability> capabilities, IndexTarget.Type indexTargetType)
    {
        return capabilities.contains(Capability.NON_FROZEN_COLLECTION) ? collectionCellValueType(baseType, indexTargetType) : baseType;
    }

    private Iterator<ByteBuffer> collectionIterator(ComplexColumnData cellData, long nowInSecs)
    {
        if (cellData == null)
            return null;

        Stream<ByteBuffer> stream = StreamSupport.stream(cellData.spliterator(), false)
                                                 .filter(cell -> cell != null && cell.isLive(nowInSecs))
                                                 .map(this::cellValue);

        if (isInetAddress())
            stream = stream.sorted((c1, c2) -> compareInet(encodeInetAddress(c1), encodeInetAddress(c2)));

        return stream.iterator();
    }

    private ByteBuffer cellValue(Cell<?> cell)
    {
        if (isNonFrozenCollection())
        {
            switch (((CollectionType<?>) columnMetadata.type).kind)
            {
                case LIST:
                    return cell.buffer();
                case SET:
                    return cell.path().get(0);
                case MAP:
                    switch (indexTargetType)
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

    private AbstractType<?> collectionCellValueType(AbstractType<?> type, IndexTarget.Type indexType)
    {
        CollectionType<?> collection = ((CollectionType<?>) type);
        switch (collection.kind)
        {
            case LIST:
                return collection.valueComparator();
            case SET:
                return collection.nameComparator();
            case MAP:
                switch (indexType)
                {
                    case KEYS:
                        return collection.nameComparator();
                    case VALUES:
                        return collection.valueComparator();
                    case KEYS_AND_VALUES:
                        return CompositeType.getInstance(collection.nameComparator(), collection.valueComparator());
                }
            default:
                throw new IllegalArgumentException("Unsupported collection type: " + collection.kind);
        }
    }

    private boolean isCompositePartition()
    {
        return capabilities.contains(Capability.COMPOSITE_PARTITION);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link InetAddressType}
     */
    private boolean isInetAddress()
    {
        return capabilities.contains(Capability.INET_ADDRESS);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link IntegerType}
     */
    private boolean isBigInteger()
    {
        return capabilities.contains(Capability.BIG_INTEGER);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link DecimalType}
     */
    private boolean isBigDecimal()
    {
        return capabilities.contains(Capability.BIG_DECIMAL);
    }

    private boolean isLong()
    {
        return capabilities.contains(Capability.LONG);
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
        return address.remaining() == INET_ADDRESS_SIZE;
    }

    /**
     * Encode a {@link InetAddress} into a fixed width 16 byte encoded value.
     * <p>
     * The encoded value is byte comparable and prefix compressible.
     * <p>
     * The encoding is done by converting ipv4 addresses to their ipv6 equivalent.
     */
    private static ByteBuffer encodeInetAddress(ByteBuffer value)
    {
        if (value.remaining() == 4)
        {
            int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
            ByteBuffer mapped = ByteBuffer.allocate(INET_ADDRESS_SIZE);
            System.arraycopy(IPV4_PREFIX, 0, mapped.array(), 0, IPV4_PREFIX.length);
            ByteBufferUtil.copyBytes(value, position, mapped, IPV4_PREFIX.length, value.remaining());
            return mapped;
        }
        return value;
    }

    /**
     * Encode a {@link BigInteger} into a fixed width 20 byte encoded value. The encoded value is byte comparable
     * and prefix compressible.
     * <p>
     * The format of the encoding is:
     * <p>
     *  The first 4 bytes contain the integer length of the {@link BigInteger} byte array
     *  with the top bit flipped for positive values.
     * <p>
     *  The remaining 16 bytes contain the 16 most significant bytes of the
     *  {@link BigInteger} byte array.
     * <p>
     *  For {@link BigInteger} values whose underlying byte array is less than
     *  16 bytes, the encoded value is sign extended.
     */
    public static ByteBuffer encodeBigInteger(ByteBuffer value)
    {
        int size = value.remaining();
        int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
        byte[] bytes = new byte[BIG_INTEGER_APPROXIMATION_BYTES];
        if (size < BIG_INTEGER_APPROXIMATION_BYTES - Integer.BYTES)
        {
            ByteBufferUtil.copyBytes(value, position, bytes, bytes.length - size, size);
            if ((bytes[bytes.length - size] & 0x80) != 0)
                Arrays.fill(bytes, Integer.BYTES, bytes.length - size, (byte)0xff);
            else
                Arrays.fill(bytes, Integer.BYTES, bytes.length - size, (byte)0x00);
        }
        else
        {
            ByteBufferUtil.copyBytes(value, position, bytes, Integer.BYTES, BIG_INTEGER_APPROXIMATION_BYTES - Integer.BYTES);
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

    public static ByteBuffer encodeDecimal(ByteBuffer value)
    {
        ByteSource bs = DecimalType.instance.asComparableBytes(value, ByteComparable.Version.OSS50);
        bs = ByteSource.cutOrRightPad(bs, DECIMAL_APPROXIMATION_BYTES, 0);
        return ByteBuffer.wrap(ByteSourceInverse.readBytes(bs, DECIMAL_APPROXIMATION_BYTES));
    }
}

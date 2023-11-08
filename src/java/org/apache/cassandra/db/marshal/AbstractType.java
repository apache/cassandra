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
package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.db.marshal.AbstractType.ComparisonType.CUSTOM;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
@Unmetered
public abstract class AbstractType<T> implements Comparator<ByteBuffer>, AssignmentTestable
{
    private final static int VARIABLE_LENGTH = -1;

    public final Comparator<ByteBuffer> reverseComparator;

    public enum ComparisonType
    {
        /**
         * This type should never be compared
         */
        NOT_COMPARABLE,
        /**
         * This type is always compared by its sequence of unsigned bytes
         */
        BYTE_ORDER,
        /**
         * This type can only be compared by calling the type's compareCustom() method, which may be expensive.
         * Support for this may be removed in a major release of Cassandra, however upgrade facilities will be
         * provided if and when this happens.
         */
        CUSTOM
    }

    public final ComparisonType comparisonType;
    public final boolean isByteOrderComparable;
    public final ValueComparators comparatorSet;

    protected AbstractType(ComparisonType comparisonType)
    {
        this.comparisonType = comparisonType;
        this.isByteOrderComparable = comparisonType == ComparisonType.BYTE_ORDER;
        reverseComparator = (o1, o2) -> AbstractType.this.compare(o2, o1);
        try
        {
            Method custom = getClass().getMethod("compareCustom", Object.class, ValueAccessor.class, Object.class, ValueAccessor.class);
            if ((custom.getDeclaringClass() == AbstractType.class) == (comparisonType == CUSTOM))
                throw new IllegalStateException((comparisonType == CUSTOM ? "compareCustom must be overridden if ComparisonType is CUSTOM"
                                                                         : "compareCustom should not be overridden if ComparisonType is not CUSTOM")
                                                + " (" + getClass().getSimpleName() + ")");
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalStateException();
        }

        comparatorSet = new ValueComparators((l, r) -> compare(l, ByteArrayAccessor.instance, r, ByteArrayAccessor.instance),
                                             (l, r) -> compare(l, ByteBufferAccessor.instance, r, ByteBufferAccessor.instance));
    }

    static <VL, VR, T extends Comparable<T>> int compareComposed(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, AbstractType<T> type)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        return type.compose(left, accessorL).compareTo(type.compose(right, accessorR));
    }

    public static List<String> asCQLTypeStringList(List<AbstractType<?>> abstractTypes)
    {
        List<String> r = new ArrayList<>(abstractTypes.size());
        for (AbstractType<?> abstractType : abstractTypes)
            r.add(abstractType.asCQL3Type().toString());
        return r;
    }

    public final T compose(ByteBuffer bytes)
    {
        return getSerializer().deserialize(bytes);
    }

    public <V> T compose(V value, ValueAccessor<V> accessor)
    {
        return getSerializer().deserialize(value, accessor);
    }

    public ByteBuffer decomposeUntyped(Object value)
    {
        return decompose((T) value);
    }

    public ByteBuffer decompose(T value)
    {
        return getSerializer().serialize(value);
    }

    /** get a string representation of the bytes used for various identifier (NOT just for log messages) */
    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        if (value == null)
            return "null";

        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(value, accessor);

        return serializer.toString(serializer.deserialize(value, accessor));
    }

    public final String getString(ByteBuffer bytes)
    {
        return getString(bytes, ByteBufferAccessor.instance);
    }

    public String toCQLString(ByteBuffer bytes)
    {
        return asCQL3Type().toCQLLiteral(bytes);
    }

    /** get a byte representation of the given string. */
    public abstract ByteBuffer fromString(String source) throws MarshalException;

    /** Given a parsed JSON string, return a byte representation of the object.
     * @param parsed the result of parsing a json string
     **/
    public abstract Term fromJSONObject(Object parsed) throws MarshalException;

    /**
     * Converts the specified value into its JSON representation.
     * <p>
     * The buffer position will stay the same.
     * </p>
     *
     * @param buffer the value to convert
     * @param protocolVersion the protocol version to use for the conversion
     * @return a JSON string representing the specified value
     */
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return '"' + Objects.toString(getSerializer().deserialize(buffer), "") + '"';
    }

    public <V> String toJSONString(V value, ValueAccessor<V> accessor, ProtocolVersion protocolVersion)
    {
        return toJSONString(accessor.toBuffer(value), protocolVersion); // FIXME
    }

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        validate(bytes, ByteBufferAccessor.instance);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        getSerializer().validate(value, accessor);
    }

    public final int compare(ByteBuffer left, ByteBuffer right)
    {
        return compare(left, ByteBufferAccessor.instance, right, ByteBufferAccessor.instance);
    }

    public final <VL, VR> int compare(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return isByteOrderComparable ? ValueAccessor.compare(left, accessorL, right, accessorR) : compareCustom(left, accessorL, right, accessorR);
    }

    /**
     * Implement IFF ComparisonType is CUSTOM
     *
     * Compares the byte representation of two instances of this class,
     * for types where this cannot be done by simple in-order comparison of the
     * unsigned bytes
     *
     * Standard Java compare semantics
     * @param left
     * @param accessorL
     * @param right
     * @param accessorR
     */
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Validate cell value. Unlike {@linkplain #validate(java.nio.ByteBuffer)},
     * cell value is passed to validate its content.
     * Usually, this is the same as validate except collection.
     *
     * @param cellValue ByteBuffer representing cell value
     * @throws MarshalException
     */
    public <V> void validateCellValue(V cellValue, ValueAccessor<V> accessor) throws MarshalException
    {
        validate(cellValue, accessor);
    }

    /* Most of our internal type should override that. */
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    public AbstractType<?> udfType()
    {
        return this;
    }

    /**
     * Same as compare except that this ignore ReversedType. This is to be use when
     * comparing 2 values to decide for a CQL condition (see Operator.isSatisfiedBy) as
     * for CQL, ReversedType is simply an "hint" to the storage engine but it does not
     * change the meaning of queries per-se.
     */
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2)
    {
        return compare(v1, v2);
    }

    public abstract TypeSerializer<T> getSerializer();

    /**
     * @return the deserializer used to deserialize the function arguments of this type.
     */
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new DefaultArgumentDeserializer(this);
    }

    /* convenience method */
    public String getString(Collection<ByteBuffer> names)
    {
        StringBuilder builder = new StringBuilder();
        for (ByteBuffer name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    public boolean isCounter()
    {
        return false;
    }

    public boolean isFrozenCollection()
    {
        return isCollection() && !isMultiCell();
    }

    public boolean isReversed()
    {
        return false;
    }

    public AbstractType<T> unwrap()
    {
        return isReversed() ? ((ReversedType<T>) this).baseType.unwrap() : this;
    }

    public static AbstractType<?> parseDefaultParameters(AbstractType<?> baseType, TypeParser parser) throws SyntaxException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }

    /**
     * Returns true if this comparator is compatible with the provided
     * previous comparator, that is if previous can safely be replaced by this.
     * A comparator cn should be compatible with a previous one cp if forall columns c1 and c2,
     * if   cn.validate(c1) and cn.validate(c2) and cn.compare(c1, c2) == v,
     * then cp.validate(c1) and cp.validate(c2) and cp.compare(c1, c2) == v.
     *
     * Note that a type should be compatible with at least itself and when in
     * doubt, keep the default behavior of not being compatible with any other comparator!
     */
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        return this.equals(previous);
    }

    /**
     * Returns true if values of the other AbstractType can be read and "reasonably" interpreted by the this
     * AbstractType. Note that this is a weaker version of isCompatibleWith, as it does not require that both type
     * compare values the same way.
     *
     * The restriction on the other type being "reasonably" interpreted is to prevent, for example, IntegerType from
     * being compatible with all other types.  Even though any byte string is a valid IntegerType value, it doesn't
     * necessarily make sense to interpret a UUID or a UTF8 string as an integer.
     *
     * Note that a type should be compatible with at least itself.
     */
    public boolean isValueCompatibleWith(AbstractType<?> previous)
    {
        AbstractType<?> thisType =          isReversed() ? ((ReversedType<?>)     this).baseType : this;
        AbstractType<?> thatType = previous.isReversed() ? ((ReversedType<?>) previous).baseType : previous;
        return thisType.isValueCompatibleWithInternal(thatType);
    }

    /**
     * Needed to handle ReversedType in value-compatibility checks.  Subclasses should implement this instead of
     * isValueCompatibleWith().
     */
    protected boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return isCompatibleWith(otherType);
    }

    /**
     * Similar to {@link #isValueCompatibleWith(AbstractType)}, but takes into account {@link Cell} encoding.
     * In particular, this method doesn't consider two types serialization compatible if one of them has fixed
     * length (overrides {@link #valueLengthIfFixed()}, and the other one doesn't.
     */
    public boolean isSerializationCompatibleWith(AbstractType<?> previous)
    {
        return isValueCompatibleWith(previous)
               && valueLengthIfFixed() == previous.valueLengthIfFixed()
               && isMultiCell() == previous.isMultiCell();
    }

    /**
     * An alternative comparison function used by CollectionsType in conjunction with CompositeType.
     *
     * This comparator is only called to compare components of a CompositeType. It gets the value of the
     * previous component as argument (or null if it's the first component of the composite).
     *
     * Unless you're doing something very similar to CollectionsType, you shouldn't override this.
     */
    public <VL, VR> int compareCollectionMembers(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, VL collectionName)
    {
        return compare(left, accessorL, right, accessorR);
    }

    public <V> void validateCollectionMember(V value, V collectionName, ValueAccessor<V> accessor) throws MarshalException
    {
        getSerializer().validate(value, accessor);
    }

    public boolean isCollection()
    {
        return false;
    }

    public boolean isUDT()
    {
        return false;
    }

    public boolean isTuple()
    {
        return false;
    }

    public boolean isVector()
    {
        return false;
    }

    public boolean isMultiCell()
    {
        return false;
    }

    public boolean isFreezable()
    {
        return false;
    }

    public AbstractType<?> freeze()
    {
        return this;
    }

    public AbstractType<?> unfreeze()
    {
        return this;
    }

    public List<AbstractType<?>> subTypes()
    {
        return Collections.emptyList();
    }

    /**
     * Returns an AbstractType instance that is equivalent to this one, but with all nested UDTs and collections
     * explicitly frozen.
     *
     * This is only necessary for {@code 2.x -> 3.x} schema migrations, and can be removed in Cassandra 4.0.
     *
     * See CASSANDRA-11609 and CASSANDRA-11613.
     */
    public AbstractType<?> freezeNestedMulticellTypes()
    {
        return this;
    }

    /**
     * Returns {@code true} for types where empty should be handled like {@code null} like {@link Int32Type}.
     */
    public boolean isEmptyValueMeaningless()
    {
        return false;
    }

    /**
     * @param ignoreFreezing if true, the type string will not be wrapped with FrozenType(...), even if this type is frozen.
     */
    public String toString(boolean ignoreFreezing)
    {
        return this.toString();
    }

    /**
     * Return a list of the "subcomponents" this type has.
     * This always return a singleton list with the type itself except for CompositeType.
     */
    public List<AbstractType<?>> getComponents()
    {
        return Collections.<AbstractType<?>>singletonList(this);
    }

    /**
     * The length of values for this type if all values are of fixed length, -1 otherwise. This has an impact on
     * serialization.
     * <lu>
     *  <li> see {@link #writeValue} </li>
     *  <li> see {@link #read} </li>
     *  <li> see {@link #writtenLength} </li>
     *  <li> see {@link #skipValue} </li>
     * </lu>
     */
    public int valueLengthIfFixed()
    {
        return VARIABLE_LENGTH;
    }

    /**
     * Checks if all values are of fixed length.
     *
     * @return {@code true} if all values are of fixed length, {@code false} otherwise.
     */
    public final boolean isValueLengthFixed()
    {
        return valueLengthIfFixed() != VARIABLE_LENGTH;
    }

    /**
     * Defines if the type allows an empty set of bytes ({@code new byte[0]}) as valid input.  The {@link #validate(Object, ValueAccessor)}
     * and {@link #compose(Object, ValueAccessor)} methods must allow empty bytes when this returns true, and must reject empty bytes
     * when this is false.
     * <p/>
     * As of this writing, the main user of this API is for testing to know what types allow empty values and what types don't,
     * so that the data that gets generated understands when {@link ByteBufferUtil#EMPTY_BYTE_BUFFER} is allowed as valid data.
     */
    public boolean allowsEmpty()
    {
        return false;
    }

    public boolean isNull(ByteBuffer bb)
    {
        return isNull(bb, ByteBufferAccessor.instance);
    }

    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        return getSerializer().isNull(buffer, accessor);
    }

    // This assumes that no empty values are passed
    public void writeValue(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        writeValue(value, ByteBufferAccessor.instance, out);
    }

    // This assumes that no empty values are passed
    public  <V> void writeValue(V value, ValueAccessor<V> accessor, DataOutputPlus out) throws IOException
    {
        assert !isNull(value, accessor) : "bytes should not be null for type " + this;
        int expectedValueLength = valueLengthIfFixed();
        if (expectedValueLength >= 0)
        {
            int actualValueLength = accessor.size(value);
            if (actualValueLength == expectedValueLength)
                accessor.write(value, out);
            else
                throw new IOException(String.format("Expected exactly %d bytes, but was %d",
                                                    expectedValueLength, actualValueLength));
        }
        else
        {
            accessor.writeWithVIntLength(value, out);
        }
    }

    public long writtenLength(ByteBuffer value)
    {
        return writtenLength(value, ByteBufferAccessor.instance);
    }

    public <V> long writtenLength(V value, ValueAccessor<V> accessor)
    {
        assert !accessor.isEmpty(value) : "bytes should not be empty for type " + this;
        return valueLengthIfFixed() >= 0
               ? accessor.size(value) // if the size is wrong, this will be detected in writeValue
               : accessor.sizeWithVIntLength(value);
    }

    public ByteBuffer readBuffer(DataInputPlus in) throws IOException
    {
        return readBuffer(in, Integer.MAX_VALUE);
    }

    public ByteBuffer readBuffer(DataInputPlus in, int maxValueSize) throws IOException
    {
        return read(ByteBufferAccessor.instance, in, maxValueSize);
    }

    public byte[] readArray(DataInputPlus in, int maxValueSize) throws IOException
    {
        return read(ByteArrayAccessor.instance, in, maxValueSize);
    }

    public <V> V read(ValueAccessor<V> accessor, DataInputPlus in, int maxValueSize) throws IOException
    {
        int length = valueLengthIfFixed();

        if (length >= 0)
            return accessor.read(in, length);
        else
        {
            int l = in.readUnsignedVInt32();
            if (l < 0)
                throw new IOException("Corrupt (negative) value length encountered");

            if (l > maxValueSize)
                throw new IOException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                                    "which is set via max_value_size in cassandra.yaml",
                                                    l, maxValueSize));

            return accessor.read(in, l);
        }
    }

    public void skipValue(DataInputPlus in) throws IOException
    {
        int length = valueLengthIfFixed();
        if (length >= 0)
            in.skipBytesFully(length);
        else
            ByteBufferUtil.skipWithVIntLength(in);
    }

    public final boolean referencesUserType(ByteBuffer name)
    {
        return referencesUserType(name, ByteBufferAccessor.instance);
    }

    public <V> boolean referencesUserType(V name, ValueAccessor<V> accessor)
    {
        return false;
    }

    /**
     * Returns an instance of this type with all references to the provided user type recursively replaced with its new
     * definition.
     */
    public AbstractType<?> withUpdatedUserType(UserType udt)
    {
        return this;
    }

    /**
     * Replace any instances of UserType with equivalent TupleType-s.
     *
     * We need it for dropped_columns, to allow safely dropping unused user types later without retaining any references
     * to them in system_schema.dropped_columns.
     */
    public AbstractType<?> expandUserTypes()
    {
        return this;
    }

    public boolean referencesDuration()
    {
        return false;
    }

    /**
     * Tests whether a CQL value having this type can be assigned to the provided receiver.
     */
    public AssignmentTestable.TestResult testAssignment(AbstractType<?> receiverType)
    {
        // testAssignement is for CQL literals and native protocol values, none of which make a meaningful
        // difference between frozen or not and reversed or not.

        if (isFreezable() && !isMultiCell())
            receiverType = receiverType.freeze();

        if (isReversed() && !receiverType.isReversed())
            receiverType = ReversedType.getInstance(receiverType);

        if (equals(receiverType))
            return AssignmentTestable.TestResult.EXACT_MATCH;

        if (receiverType.isValueCompatibleWith(this))
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
    }

    /**
     * Produce a byte-comparable representation of the given value, i.e. a sequence of bytes that compares the same way
     * using lexicographical unsigned byte comparison as the original value using the type's comparator.
     *
     * We use a slightly stronger requirement to be able to use the types in tuples. Precisely, for any pair x, y of
     * non-equal valid values of this type and any bytes b1, b2 between 0x10 and 0xEF,
     * (+ stands for concatenation)
     *   compare(x, y) == compareLexicographicallyUnsigned(asByteComparable(x)+b1, asByteComparable(y)+b2)
     * (i.e. the values compare like the original type, and an added 0x10-0xEF byte at the end does not change that) and:
     *   asByteComparable(x)+b1 is not a prefix of asByteComparable(y)      (weakly prefix free)
     * (i.e. a valid representation of a value may be a prefix of another valid representation of a value only if the
     * following byte in the latter is smaller than 0x10 or larger than 0xEF). These properties are trivially true if
     * the encoding compares correctly and is prefix free, but also permits a little more freedom that enables somewhat
     * more efficient encoding of arbitrary-length byte-comparable blobs.
     *
     * Depending on the type, this method can be called for null or empty input, in which case the output is allowed to
     * be null (the clustering/tuple encoding will accept and handle it).
     */
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V value, ByteComparable.Version version)
    {
        if (isByteOrderComparable)
        {
            // When a type is byte-ordered on its own, we only need to escape it, so that we can include it in
            // multi-component types and make the encoding weakly-prefix-free.
            return ByteSource.of(accessor, value, version);
        }
        else
            // default is only good for byte-comparables
            throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement asComparableBytes");
    }

    public final ByteSource asComparableBytes(ByteBuffer byteBuffer, ByteComparable.Version version)
    {
        return asComparableBytes(ByteBufferAccessor.instance, byteBuffer, version);
    }

    /**
     * Translates the given byte-ordered representation to the common, non-byte-ordered binary representation of a
     * payload for this abstract type (the latter, common binary representation is what we mostly work with in the
     * storage engine internals). If the given bytes don't correspond to the encoding of some payload value for this
     * abstract type, an {@link IllegalArgumentException} may be thrown.
     *
     * @param accessor value accessor used to construct the value.
     * @param comparableBytes A byte-ordered representation (presumably of a payload for this abstract type).
     * @param version The byte-comparable version used to construct the representation.
     * @return A of a payload for this abstract type, corresponding to the given byte-ordered representation,
     *         constructed using the supplied value accessor.
     *
     * @see #asComparableBytes
     */
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        if (isByteOrderComparable)
            return accessor.valueOf(ByteSourceInverse.getUnescapedBytes(comparableBytes));
        else
            throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement fromComparableBytes");
    }

    public final ByteBuffer fromComparableBytes(ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return fromComparableBytes(ByteBufferAccessor.instance, comparableBytes, version);
    }

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     *
     * Note that for backwards compatibility this includes the full classname.
     * For CQL purposes the short name is fine.
     */
    @Override
    public String toString()
    {
        return getClass().getName();
    }

    public void checkComparable()
    {
        switch (comparisonType)
        {
            case NOT_COMPARABLE:
                throw new IllegalArgumentException(this + " cannot be used in comparisons, so cannot be used as a clustering column");
        }
    }

    public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        return testAssignment(receiver.type);
    }

    @Override
    public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
    {
        return this;
    }

    /**
     * @return A fixed, serialized value to be used when the column is masked, to be returned instead of the real value.
     */
    public ByteBuffer getMaskedValue()
    {
        throw new UnsupportedOperationException("There isn't a defined masked value for type " + asCQL3Type());
    }

    /**
     * {@link ArgumentDeserializer} that uses the type deserialization.
     */
    protected static class DefaultArgumentDeserializer implements ArgumentDeserializer
    {
        private final AbstractType<?> type;

        public DefaultArgumentDeserializer(AbstractType<?> type)
        {
            this.type = type;
        }

        @Override
        public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer)
        {
            if (buffer == null || (!buffer.hasRemaining() && type.isEmptyValueMeaningless()))
                return null;

            return type.compose(buffer);
        }
    }
}

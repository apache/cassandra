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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.Difference;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UserTypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.cql3.ColumnIdentifier.maybeQuote;

/**
 * A user defined type.
 * <p>
 * A user type is really just a tuple type on steroids.
 */
public class UserType extends TupleType implements SchemaElement
{
    private static final Logger logger = LoggerFactory.getLogger(UserType.class);

    private static final ConflictBehavior CONFLICT_BEHAVIOR = ConflictBehavior.get();

    public final String keyspace;
    public final ByteBuffer name;
    private final List<FieldIdentifier> fieldNames;
    private final List<String> stringFieldNames;
    private final UserTypeSerializer serializer;

    public UserType(String keyspace,
                    ByteBuffer name,
                    List<FieldIdentifier> fieldNames,
                    List<AbstractType<?>> fieldTypes,
                    boolean isMultiCell)
    {
        this(keyspace, name, fieldNames, ImmutableList.copyOf(fieldTypes), isMultiCell);
    }

    public UserType(String keyspace,
                    ByteBuffer name,
                    List<FieldIdentifier> fieldNames,
                    ImmutableList<AbstractType<?>> fieldTypes,
                    boolean isMultiCell)
    {
        super(fieldTypes, isMultiCell);
        assert fieldNames.size() == fieldTypes.size();
        this.keyspace = keyspace;
        this.name = name;
        this.fieldNames = fieldNames;
        this.stringFieldNames = new ArrayList<>(fieldNames.size());

        LinkedHashMap<String , TypeSerializer<?>> fieldSerializers = new LinkedHashMap<>(fieldTypes.size());
        for (int i = 0, m = fieldNames.size(); i < m; i++)
        {
            String stringFieldName = fieldNames.get(i).toString();
            stringFieldNames.add(stringFieldName);
            TypeSerializer<?> existing = fieldSerializers.put(stringFieldName, fieldTypes.get(i).getSerializer());
            if (existing != null)
                CONFLICT_BEHAVIOR.onConflict(keyspace, getNameAsString(), stringFieldName);
        }
        this.serializer = new UserTypeSerializer(fieldSerializers);
    }

    public static UserType getInstance(TypeParser parser)
    {
        Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType<?>>>> params = parser.getUserTypeParameters();
        String keyspace = params.left.left;
        ByteBuffer name = params.left.right;
        List<FieldIdentifier> columnNames = new ArrayList<>(params.right.size());
        List<AbstractType<?>> columnTypes = new ArrayList<>(params.right.size());
        for (Pair<ByteBuffer, AbstractType<?>> p : params.right)
        {
            columnNames.add(new FieldIdentifier(p.left));
            columnTypes.add(p.right);
        }

        return new UserType(keyspace, name, columnNames, columnTypes, true);
    }

    @Override
    public UserType with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        return new UserType(keyspace, name, fieldNames, subTypes, isMultiCell);
    }

    @Override
    public UserType overrideKeyspace(Function<String, String> overrideKeyspace)
    {
        String newKeyspace = overrideKeyspace.apply(keyspace);
        if (newKeyspace.equals(keyspace))
            return this;

        return new UserType(newKeyspace,
                            name,
                            fieldNames,
                            subTypes.stream().map(t -> t.overrideKeyspace(overrideKeyspace)).collect(Collectors.toList()),
                            isMultiCell());
    }

    @Override
    public boolean isUDT()
    {
        return true;
    }

    public boolean isTuple()
    {
        return false;
    }

    public AbstractType<?> fieldType(int i)
    {
        return type(i);
    }

    public List<AbstractType<?>> fieldTypes()
    {
        return subTypes;
    }

    public FieldIdentifier fieldName(int i)
    {
        return fieldNames.get(i);
    }

    public String fieldNameAsString(int i)
    {
        return stringFieldNames.get(i);
    }

    public List<FieldIdentifier> fieldNames()
    {
        return fieldNames;
    }

    public String getNameAsString()
    {
        return UTF8Type.instance.compose(name);
    }

    public int fieldPosition(FieldIdentifier fieldName)
    {
        return fieldNames.indexOf(fieldName);
    }

    public CellPath cellPathForField(FieldIdentifier fieldName)
    {
        // we use the field position instead of the field name to allow for field renaming in ALTER TYPE statements
        return CellPath.create(ByteBufferUtil.bytes((short)fieldPosition(fieldName)));
    }

    public <V> void validateCell(Cell<V> cell) throws MarshalException
    {
        if (isMultiCell())
        {
            ByteBuffer path = cell.path().get(0);
            nameComparator().validate(path);
            Short fieldPosition = nameComparator().getSerializer().deserialize(path);
            fieldType(fieldPosition).validate(cell.value(), cell.accessor());
        }
        else
        {
            validate(cell.value(), cell.accessor());
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<String, Object> map = (Map<String, Object>) parsed;

        Json.handleCaseSensitivity(map);

        List<Term> terms = new ArrayList<>(size());

        Set<String> keys = map.keySet();
        assert keys.isEmpty() || keys.iterator().next() != null;

        int foundValues = 0;
        for (int i = 0; i < size(); i++)
        {
            Object value = map.get(stringFieldNames.get(i));
            if (value == null)
            {
                terms.add(Constants.NULL_VALUE);
            }
            else
            {
                terms.add(subTypes.get(i).fromJSONObject(value));
                foundValues += 1;
            }
        }

        // check for extra, unrecognized fields
        if (foundValues != map.size())
        {
            for (String fieldName : keys)
            {
                if (!stringFieldNames.contains(fieldName))
                    throw new MarshalException(String.format(
                            "Unknown field '%s' in value of user defined type %s", fieldName, getNameAsString()));
            }
        }

        return new UserTypes.DelayedValue(this, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer[] buffers = split(ByteBufferAccessor.instance, buffer);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            String name = stringFieldNames.get(i);
            if (!name.equals(name.toLowerCase(Locale.US)))
                name = '"' + name + '"';

            sb.append('"');
            sb.append(Json.quoteAsJsonString(name));
            sb.append("\": ");

            ByteBuffer valueBuffer = (i >= buffers.length) ? null : buffers[i];
            if (valueBuffer == null)
                sb.append("null");
            else
                sb.append(subTypes.get(i).toJSONString(valueBuffer, protocolVersion));
        }
        return sb.append('}').toString();
    }

    @Override
    public UserType freeze()
    {
        return (UserType) super.freeze();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyspace, name, fieldNames, subTypes, isMultiCell());
    }

    @Override
    protected boolean isCompatibleWhenFrozenWith(AbstractType<?> previous)
    {
        return super.isCompatibleWhenFrozenWith(previous) && keyspace.equals(((UserType)previous).keyspace);
    }

    @Override
    protected boolean isCompatibleWhenNonFrozenWith(AbstractType<?> previous)
    {
        return super.isCompatibleWhenNonFrozenWith(previous) && keyspace.equals(((UserType)previous).keyspace);
    }

    @Override
    protected boolean isValueCompatibleWhenFrozenWith(AbstractType<?> previous)
    {
        return super.isValueCompatibleWhenFrozenWith(previous) && keyspace.equals(((UserType)previous).keyspace);
    }

    @Override
    protected boolean equalsNoFrozenNoSubtypes(AbstractType<?> other)
    {
        UserType that = (UserType)other;
        return super.equalsNoFrozenNoSubtypes(other)
               && this.name.equals(that.name)
               && this.fieldNames.equals(that.fieldNames)
               && this.keyspace.equals(that.keyspace);
    }

    public Optional<Difference> compare(UserType other)
    {
        if (!equalsNoFrozenNoSubtypes(other) || isMultiCell() != other.isMultiCell())
            return Optional.of(Difference.SHALLOW);

        boolean differsDeeply = false;

        for (int i = 0; i < fieldTypes().size(); i++)
        {
            AbstractType<?> thisType = fieldType(i);
            AbstractType<?> thatType = other.fieldType(i);

            if (!thisType.equals(thatType))
            {
                if (thisType.asCQL3Type().toString().equals(thatType.asCQL3Type().toString()))
                    differsDeeply = true;
                else
                    return Optional.of(Difference.SHALLOW);
            }
        }

        return differsDeeply ? Optional.of(Difference.DEEP) : Optional.empty();
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.UserDefined.create(this);
    }

    @Override
    public <V> boolean referencesUserType(V name, ValueAccessor<V> accessor)
    {
        return this.name.equals(name) || any(fieldTypes(), t -> t.referencesUserType(name, accessor));
    }

    @Override
    public AbstractType<?> expandUserTypes()
    {
        return new TupleType(ImmutableList.copyOf(transform(subTypes, AbstractType::expandUserTypes)), isMultiCell());
    }

    @Override
    public UserType withUpdatedUserType(UserType udt)
    {
        // If we're not the UDT to update, we can rely on the default implementation
        if (!name.equals(udt.name))
            return (UserType) super.withUpdatedUserType(udt);

        assert udt.isMultiCell();
        // The type we're updating may be frozen, why the updated user type will never be (a UDT is never frozen in
        // its definition, only in its use). So if we are frozen, we should froze the UDT we switch to.
        return isMultiCell() ? udt : udt.freeze();
    }

    @Override
    public boolean referencesDuration()
    {
        return fieldTypes().stream().anyMatch(AbstractType::referencesDuration);
    }

    @Override
    protected String stringifyTypeParameters(boolean ignoreFreezing)
    {
        return TypeParser.stringifyUserTypeParameters(keyspace, name, fieldNames, subTypes, ignoreFreezing || !isMultiCell());
    }

    public String getCqlTypeName()
    {
        return String.format("%s.%s", maybeQuote(keyspace), maybeQuote(getNameAsString()));
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return serializer;
    }

    @Override
    public SchemaElementType elementType()
    {
        return SchemaElementType.TYPE;
    }

    @Override
    public String elementKeyspace()
    {
        return keyspace;
    }

    @Override
    public String elementName()
    {
        return getNameAsString();
    }

    @Override
    public String toCqlString(boolean withInternals, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("CREATE TYPE ");

        if (ifNotExists)
        {
            builder.append("IF NOT EXISTS ");
        }

        builder.appendQuotingIfNeeded(keyspace)
               .append('.')
               .appendTypeQuotingIfNeeded(getNameAsString())
               .append(" (")
               .newLine()
               .increaseIndent();

        for (int i = 0; i < size(); i++)
        {
            if (i > 0)
                builder.append(",")
                       .newLine();

            builder.appendQuotingIfNeeded(fieldNameAsString(i))
                   .append(' ')
                   .append(fieldType(i));
        }

        builder.newLine()
               .decreaseIndent()
               .append(");");

        return builder.toString();
    }

    private enum ConflictBehavior
    {
        LOG {
            void onConflict(String keyspace, String name, String fieldName)
            {
                logger.error("Duplicate names found in UDT {}.{} for column {}",
                             maybeQuote(keyspace), maybeQuote(name), maybeQuote(fieldName));
            }
        },
        REJECT {
            @Override
            void onConflict(String keyspace, String name, String fieldName)
            {

                throw new AssertionError(String.format("Duplicate names found in UDT %s.%s for column %s; " +
                                                       "to resolve set -D" + UDT_CONFLICT_BEHAVIOR + "=LOG on startup and remove the type",
                                                       maybeQuote(keyspace), maybeQuote(name), maybeQuote(fieldName)));
            }
        };

        private static final String UDT_CONFLICT_BEHAVIOR = "cassandra.type.udt.conflict_behavior";

        abstract void onConflict(String keyspace, String name, String fieldName);

        static ConflictBehavior get()
        {
            String value = System.getProperty(UDT_CONFLICT_BEHAVIOR, REJECT.name());
            return ConflictBehavior.valueOf(value);
        }
    }
}

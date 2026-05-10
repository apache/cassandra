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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.SchemaElement;
import org.apache.cassandra.cql3.statements.SchemaDescriptionsUtil;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.MultiElements;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.Difference;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UserTypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.config.CassandraRelevantProperties.TYPE_UDT_CONFLICT_BEHAVIOR;
import static org.apache.cassandra.cql3.ColumnIdentifier.maybeQuote;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * A user defined type.
 *
 * A user type is really just a tuple type on steroids.
 */
public class UserType extends TupleType implements SchemaElement
{
    private static final Logger logger = LoggerFactory.getLogger(UserType.class);

    private static final ConflictBehavior CONFLICT_BEHAVIOR = ConflictBehavior.get();
    private static final String EMPTY_COMMENT = "";
    private static final String EMPTY_SECURITY_LABEL = "";

    public final String keyspace;
    public final ByteBuffer name;
    public final String comment;
    public final String securityLabel;
    private final List<FieldIdentifier> fieldNames;
    private final List<String> stringFieldNames;
    private final Map<FieldIdentifier, String> fieldComments;
    private final Map<FieldIdentifier, String> fieldSecurityLabels;
    private final boolean isMultiCell;
    private final UserTypeSerializer serializer;

    public UserType(String keyspace, ByteBuffer name, List<FieldIdentifier> fieldNames, List<AbstractType<?>> fieldTypes, boolean isMultiCell)
    {
        this(keyspace, name, fieldNames, fieldTypes, isMultiCell, EMPTY_COMMENT, EMPTY_SECURITY_LABEL, Collections.emptyMap(), Collections.emptyMap());
    }

    public UserType(String keyspace, ByteBuffer name, List<FieldIdentifier> fieldNames, List<AbstractType<?>> fieldTypes, boolean isMultiCell, String comment, String securityLabel)
    {
        this(keyspace, name, fieldNames, fieldTypes, isMultiCell, comment, securityLabel, Collections.emptyMap(), Collections.emptyMap());
    }

    public UserType(String keyspace,
                    ByteBuffer name,
                    List<FieldIdentifier> fieldNames,
                    List<AbstractType<?>> fieldTypes,
                    boolean isMultiCell,
                    String comment,
                    String securityLabel,
                    Map<FieldIdentifier, String> fieldComments,
                    Map<FieldIdentifier, String> fieldSecurityLabels)
    {
        super(fieldTypes, false);
        assert fieldNames.size() == fieldTypes.size();
        this.keyspace = keyspace;
        this.name = name;
        this.comment = comment == null ? EMPTY_COMMENT : comment;
        this.securityLabel = securityLabel == null ? EMPTY_SECURITY_LABEL : securityLabel;
        this.fieldNames = fieldNames;
        this.stringFieldNames = new ArrayList<>(fieldNames.size());
        this.fieldComments = Collections.unmodifiableMap(new HashMap<>(fieldComments));
        this.fieldSecurityLabels = Collections.unmodifiableMap(new HashMap<>(fieldSecurityLabels));
        this.isMultiCell = isMultiCell;

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
        Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> params = parser.getUserTypeParameters();
        String keyspace = params.left.left;
        ByteBuffer name = params.left.right;
        List<FieldIdentifier> columnNames = new ArrayList<>(params.right.size());
        List<AbstractType<?>> columnTypes = new ArrayList<>(params.right.size());
        for (Pair<ByteBuffer, AbstractType> p : params.right)
        {
            columnNames.add(new FieldIdentifier(p.left));
            columnTypes.add(p.right);
        }

        return new UserType(keyspace, name, columnNames, columnTypes, true, EMPTY_COMMENT, EMPTY_SECURITY_LABEL);
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

    @Override
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public boolean isFreezable()
    {
        return true;
    }

    public AbstractType<?> fieldType(int i)
    {
        return type(i);
    }

    public AbstractType<?> fieldType(CellPath path)
    {
        int field = ByteBufferUtil.getUnsignedShort(path.get(0), 0);
        return fieldType(field);
    }

    public List<AbstractType<?>> fieldTypes()
    {
        return types;
    }

    public FieldIdentifier fieldName(int i)
    {
        return fieldNames.get(i);
    }

    public FieldIdentifier fieldName(CellPath path)
    {
        return fieldNames.get(fieldPosition(path));
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

    public int fieldPosition(CellPath path)
    {
        return Preconditions.checkElementIndex(ByteBufferUtil.getUnsignedShort(path.get(0), 0), fieldNames.size());
    }

    public CellPath cellPathForField(FieldIdentifier fieldName)
    {
        // we use the field position instead of the field name to allow for field renaming in ALTER TYPE statements
        return CellPath.create(ByteBufferUtil.bytes((short)fieldPosition(fieldName)));
    }

    public ShortType nameComparator()
    {
        return ShortType.instance;
    }

    public ByteBuffer serializeForNativeProtocol(Iterator<Cell<?>> cells)
    {
        return serializeForNativeProtocol(cells, ByteBufferAccessor.instance);
    }

    public byte[] serializeForNativeProtocolAsByteArrays(Iterator<Cell<?>> cells)
    {
        return serializeForNativeProtocol(cells, ByteArrayAccessor.instance);
    }

    public <V> V serializeForNativeProtocol(Iterator<Cell<?>> cells, ValueAccessor<V> accessor)
    {
        assert isMultiCell;

        List<V> components = new ArrayList<>(size());
        while (cells.hasNext())
        {
            Cell<?> cell = cells.next();

            // handle null fields that aren't at the end
            short fieldPositionOfCell = ByteBufferUtil.toShort(cell.path().get(0));
            while (components.size() < fieldPositionOfCell)
                components.add(null);

            components.add(getValue(cell, accessor));
        }

        // append trailing nulls for missing cells
        while (components.size() < size())
            components.add(null);

        return pack(components, accessor);
    }

    private static <V1, V2> V2 getValue(Cell<V1> cell, ValueAccessor<V2> targetAccessor)
    {
        return targetAccessor.convert(cell.value(), cell.accessor());
    }

    public <V> void validateCell(Cell<V> cell) throws MarshalException
    {
        if (isMultiCell)
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
            parsed = JsonUtils.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<String, Object> map = (Map<String, Object>) parsed;

        JsonUtils.handleCaseSensitivity(map);

        List<Term> terms = new ArrayList<>(types.size());

        Set keys = map.keySet();
        assert keys.isEmpty() || keys.iterator().next() instanceof String;

        int foundValues = 0;
        for (int i = 0; i < types.size(); i++)
        {
            Object value = map.get(stringFieldNames.get(i));
            if (value == null)
            {
                terms.add(Constants.NULL_VALUE);
            }
            else
            {
                terms.add(types.get(i).fromJSONObject(value));
                foundValues += 1;
            }
        }

        // check for extra, unrecognized fields
        if (foundValues != map.size())
        {
            for (Object fieldName : keys)
            {
                if (!stringFieldNames.contains(fieldName))
                    throw new MarshalException(String.format(
                            "Unknown field '%s' in value of user defined type %s", fieldName, getNameAsString()));
            }
        }

        return new MultiElements.DelayedValue(this, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        List<ByteBuffer> buffers = unpack(buffer);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < types.size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            String name = stringFieldNames.get(i);
            if (!name.equals(toLowerCaseLocalized(name)))
                name = "\"" + name + "\"";

            sb.append('"');
            sb.append(JsonUtils.quoteAsJsonString(name));
            sb.append("\": ");

            ByteBuffer valueBuffer = (i >= buffers.size()) ? null : buffers.get(i);
            if (valueBuffer == null)
                sb.append("null");
            else
                sb.append(types.get(i).toJSONString(valueBuffer, protocolVersion));
        }
        return sb.append("}").toString();
    }

    @Override
    public UserType freeze()
    {
        return isMultiCell ? new UserType(keyspace, name, fieldNames, fieldTypes(), false, comment, securityLabel, fieldComments, fieldSecurityLabels) : this;
    }

    @Override
    public UserType unfreeze()
    {
        return isMultiCell ? this : new UserType(keyspace, name, fieldNames, fieldTypes(), true, comment, securityLabel, fieldComments, fieldSecurityLabels);
    }

    @Override
    public AbstractType<?> freezeNestedMulticellTypes()
    {
        if (!isMultiCell())
            return this;

        // the behavior here doesn't exactly match the method name: we want to freeze everything inside of UDTs
        List<AbstractType<?>> newTypes = fieldTypes().stream()
                .map(subtype -> (subtype.isFreezable() && subtype.isMultiCell() ? subtype.freeze() : subtype))
                .collect(Collectors.toList());

        return new UserType(keyspace, name, fieldNames, newTypes, isMultiCell);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyspace, name, fieldNames, types, isMultiCell, comment, securityLabel, fieldComments, fieldSecurityLabels);
    }

    @Override
    public boolean isValueCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!(previous instanceof UserType))
            return false;

        UserType other = (UserType) previous;
        if (isMultiCell != other.isMultiCell())
            return false;

        if (!keyspace.equals(other.keyspace))
            return false;

        Iterator<AbstractType<?>> thisTypeIter = types.iterator();
        Iterator<AbstractType<?>> previousTypeIter = other.types.iterator();
        while (thisTypeIter.hasNext() && previousTypeIter.hasNext())
        {
            if (!thisTypeIter.next().isCompatibleWith(previousTypeIter.next()))
                return false;
        }

        // it's okay for the new type to have additional fields, but not for the old type to have additional fields
        return !previousTypeIter.hasNext();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o.getClass() != UserType.class)
            return false;

        UserType that = (UserType)o;

        return equalsWithoutTypes(that) && types.equals(that.types);
    }

    private boolean equalsWithoutTypes(UserType other)
    {
        return name.equals(other.name)
            && fieldNames.equals(other.fieldNames)
            && keyspace.equals(other.keyspace)
            && isMultiCell == other.isMultiCell
            && comment.equals(other.comment)
            && securityLabel.equals(other.securityLabel)
            && fieldComments.equals(other.fieldComments)
            && fieldSecurityLabels.equals(other.fieldSecurityLabels);
    }

    public boolean equalsWithOutKs(UserType other)
    {
        // Doesn't consider comments or security labels at either the
        // type or field level as this method is used to check compatibility of
        // UserTypes in different keyspaces for validation in CopyTableStatement
        return name.equals(other.name)
            && fieldNames.equals(other.fieldNames)
            && types.equals(other.types)
            && isMultiCell == other.isMultiCell;
    }

    public Optional<Difference> compare(UserType other)
    {
        if (!equalsWithoutTypes(other))
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
    public UserType withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        // preserve frozen/non-frozen status of the updated UDT
        if (name.equals(udt.name))
        {
            return isMultiCell == udt.isMultiCell
                 ? udt
                 : new UserType(keyspace, name, udt.fieldNames(), udt.fieldTypes(), isMultiCell, udt.comment, udt.securityLabel, udt.fieldComments, udt.fieldSecurityLabels);
        }

        return new UserType(keyspace,
                            name,
                            fieldNames,
                            Lists.newArrayList(transform(fieldTypes(), t -> t.withUpdatedUserType(udt))),
                            isMultiCell(),
                            comment,
                            securityLabel,
                            fieldComments,
                            fieldSecurityLabels);
    }

    public UserType withComment(String comment)
    {
        return new UserType(keyspace, name, fieldNames, fieldTypes(), isMultiCell(), comment, securityLabel, fieldComments, fieldSecurityLabels);
    }

    public UserType withSecurityLabel(String securityLabel)
    {
        return new UserType(keyspace, name, fieldNames, fieldTypes(), isMultiCell(), comment, securityLabel, fieldComments, fieldSecurityLabels);
    }

    public UserType withFieldComment(FieldIdentifier fieldName, String fieldComment)
    {
        if (fieldPosition(fieldName) == -1)
            throw new IllegalArgumentException(String.format("Field '%s' doesn't exist in type '%s.%s'", fieldName, keyspace, getNameAsString()));

        Map<FieldIdentifier, String> newFieldComments = new HashMap<>(fieldComments);
        if (fieldComment == null || fieldComment.isEmpty())
            newFieldComments.remove(fieldName);
        else
            newFieldComments.put(fieldName, fieldComment);

        return new UserType(keyspace, name, fieldNames, fieldTypes(), isMultiCell(), comment, securityLabel, newFieldComments, fieldSecurityLabels);
    }

    public UserType withFieldSecurityLabel(FieldIdentifier fieldName, String fieldSecurityLabel)
    {
        if (fieldPosition(fieldName) == -1)
            throw new IllegalArgumentException(String.format("Field '%s' doesn't exist in type '%s.%s'", fieldName, keyspace, getNameAsString()));

        Map<FieldIdentifier, String> newFieldSecurityLabels = new HashMap<>(fieldSecurityLabels);
        if (fieldSecurityLabel == null || fieldSecurityLabel.isEmpty())
            newFieldSecurityLabels.remove(fieldName);
        else
            newFieldSecurityLabels.put(fieldName, fieldSecurityLabel);

        return new UserType(keyspace, name, fieldNames, fieldTypes(), isMultiCell(), comment, securityLabel, fieldComments, newFieldSecurityLabels);
    }

    public String fieldComment(FieldIdentifier fieldName)
    {
        return fieldComments.getOrDefault(fieldName, "");
    }

    public String fieldSecurityLabel(FieldIdentifier fieldName)
    {
        return fieldSecurityLabels.getOrDefault(fieldName, "");
    }

    @Override
    public boolean referencesDuration()
    {
        return fieldTypes().stream().anyMatch(f -> f.referencesDuration());
    }

    @Override
    public int compareCQL(ComplexColumnData columnData, List<ByteBuffer> fields)
    {
        Iterator<Cell<?>> cellIter = columnData.iterator();
        int i = 0;
        while (cellIter.hasNext())
        {
            if (i == fields.size())
                return 1;

            Cell<?> cell = cellIter.next();
            short position = ByteBufferUtil.toShort(cell.path().get(0));

            while (i < position)
            {
                if (i == fields.size())
                    return 1;

                if (fields.get(i++) != null)
                    return -1;
            }

            ByteBuffer fieldValue = fields.get(i);

            if (fieldValue == null)
                return 1;

            int comparison = type(i++).compare(cell.buffer(), fieldValue);
            if (comparison != 0)
                return comparison;
        }

        while(i < fields.size())
        {
            if (fields.get(i++) != null)
                return -1;
        }

        return 0;
    }

    @Override
    public AbstractType<?> elementType(ByteBuffer keyOrIndex)
    {
        return type(fieldPosition(new FieldIdentifier(keyOrIndex)));
    }

    @Override
    public ByteBuffer getElement(@Nullable ColumnData columnData, ByteBuffer keyOrIndex)
    {
        if (columnData == null)
            return null;

        FieldIdentifier field = new FieldIdentifier(keyOrIndex);

        if (isMultiCell())
        {
            Cell<?> cell = ((ComplexColumnData) columnData).getCell(cellPathForField(field));
            return cell == null ? null : cell.buffer();
        }

        return unpack(((Cell<?>) columnData).buffer()).get(fieldPosition(field));
    }

    @Override
    public String toString()
    {
        return this.toString(false);
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyUserTypeParameters(keyspace, name, fieldNames, types, ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
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
    public List<ByteBuffer> filterSortAndValidateElements(List<ByteBuffer> buffers)
    {
        return filterSortAndValidateElements(buffers, ByteBufferUtil.UNSET_BYTE_BUFFER, ByteBufferAccessor.instance);
    }

    @Override
    public List<byte[]> filterSortAndValidateElementsFromArrays(List<byte[]> buffers)
    {
        return filterSortAndValidateElements(buffers, ByteArrayUtil.UNSET_BYTE_ARRAY, ByteArrayAccessor.instance);
    }

    private <T> List<T> filterSortAndValidateElements(List<T> buffers, T unsetValue, ValueAccessor<T> valueAccessor)
    {
        if (buffers.size() > size())
            throw new MarshalException(String.format("UDT value contained too many fields (expected %s, got %s)", size(), buffers.size()));

        for (int i = 0; i < buffers.size(); i++)
        {
            // Since a frozen UDT value is always written in its entirety Cassandra can't preserve a pre-existing
            // value by 'not setting' the new value. Reject the query.
            T buffer = buffers.get(i);
            if (buffer == null)
                continue;
            if (!isMultiCell() && buffer == unsetValue)
                throw new MarshalException(String.format("Invalid unset value for field '%s' of user defined type %s", fieldNameAsString(i), getNameAsString()));
            type(i).validate(buffer, valueAccessor);
        }

        return buffers;
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
    public String toCqlString(boolean withWarnings, boolean withInternals, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("CREATE TYPE ");

        if (ifNotExists)
        {
            builder.append("IF NOT EXISTS ");
        }

        builder.appendQuotingIfNeeded(keyspace)
               .append('.')
               .appendQuotingIfNeeded(getNameAsString())
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

    @Override
    public String describe(boolean withWarnings, boolean withInternals, boolean ifNotExists)
    {
        String baseStatement = toCqlString(withWarnings, withInternals, ifNotExists);
        StringBuilder result = new StringBuilder(baseStatement);
        SchemaDescriptionsUtil.appendCommentOnType(result, this);
        SchemaDescriptionsUtil.appendSecurityLabelOnType(result, this);
        return result.toString();
    }

    @Override
    protected String componentOrFieldName(int i)
    {
        return "field " + fieldName(i);
    }

    @Override
    public boolean isConstrainable()
    {
        return false;
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
                                                       "to resolve set -D%s=LOG on startup and remove the type",
                                                       maybeQuote(keyspace), maybeQuote(name), maybeQuote(fieldName), TYPE_UDT_CONFLICT_BEHAVIOR.getKey()));
            }
        };

        abstract void onConflict(String keyspace, String name, String fieldName);

        static ConflictBehavior get()
        {
            String value = TYPE_UDT_CONFLICT_BEHAVIOR.getString(REJECT.name());
            return ConflictBehavior.valueOf(value);
        }
    }
}

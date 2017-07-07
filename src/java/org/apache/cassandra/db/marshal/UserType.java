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
import java.util.stream.Collectors;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UserTypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A user defined type.
 *
 * A user type is really just a tuple type on steroids.
 */
public class UserType extends TupleType
{
    private static final Logger logger = LoggerFactory.getLogger(UserType.class);

    public final String keyspace;
    public final ByteBuffer name;
    private final List<FieldIdentifier> fieldNames;
    private final List<String> stringFieldNames;
    private final boolean isMultiCell;
    private final UserTypeSerializer serializer;

    public UserType(String keyspace, ByteBuffer name, List<FieldIdentifier> fieldNames, List<AbstractType<?>> fieldTypes, boolean isMultiCell)
    {
        super(fieldTypes, false);
        assert fieldNames.size() == fieldTypes.size();
        this.keyspace = keyspace;
        this.name = name;
        this.fieldNames = fieldNames;
        this.stringFieldNames = new ArrayList<>(fieldNames.size());
        this.isMultiCell = isMultiCell;

        LinkedHashMap<String , TypeSerializer<?>> fieldSerializers = new LinkedHashMap<>(fieldTypes.size());
        for (int i = 0, m = fieldNames.size(); i < m; i++)
        {
            String stringFieldName = fieldNames.get(i).toString();
            stringFieldNames.add(stringFieldName);
            fieldSerializers.put(stringFieldName, fieldTypes.get(i).getSerializer());
        }
        this.serializer = new UserTypeSerializer(fieldSerializers);
    }

    public static UserType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
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

        return new UserType(keyspace, name, columnNames, columnTypes, true);
    }

    @Override
    public boolean isUDT()
    {
        return true;
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

    public List<AbstractType<?>> fieldTypes()
    {
        return types;
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

    public ShortType nameComparator()
    {
        return ShortType.instance;
    }

    public ByteBuffer serializeForNativeProtocol(Iterator<Cell> cells, ProtocolVersion protocolVersion)
    {
        assert isMultiCell;

        ByteBuffer[] components = new ByteBuffer[size()];
        short fieldPosition = 0;
        while (cells.hasNext())
        {
            Cell cell = cells.next();

            // handle null fields that aren't at the end
            short fieldPositionOfCell = ByteBufferUtil.toShort(cell.path().get(0));
            while (fieldPosition < fieldPositionOfCell)
                components[fieldPosition++] = null;

            components[fieldPosition++] = cell.value();
        }

        // append trailing nulls for missing cells
        while (fieldPosition < size())
            components[fieldPosition++] = null;

        return TupleType.buildValue(components);
    }

    public void validateCell(Cell cell) throws MarshalException
    {
        if (isMultiCell)
        {
            ByteBuffer path = cell.path().get(0);
            nameComparator().validate(path);
            Short fieldPosition = nameComparator().getSerializer().deserialize(path);
            fieldType(fieldPosition).validate(cell.value());
        }
        else
        {
            validate(cell.value());
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

        return new UserTypes.DelayedValue(this, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer[] buffers = split(buffer);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < types.size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            String name = stringFieldNames.get(i);
            if (!name.equals(name.toLowerCase(Locale.US)))
                name = "\"" + name + "\"";

            sb.append('"');
            sb.append(Json.quoteAsJsonString(name));
            sb.append("\": ");

            ByteBuffer valueBuffer = (i >= buffers.length) ? null : buffers[i];
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
        if (isMultiCell)
            return new UserType(keyspace, name, fieldNames, fieldTypes(), false);
        else
            return this;
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
        return Objects.hashCode(keyspace, name, fieldNames, types, isMultiCell);
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
        return o instanceof UserType && equals(o, false);
    }

    @Override
    public boolean equals(Object o, boolean ignoreFreezing)
    {
        if(!(o instanceof UserType))
            return false;

        UserType that = (UserType)o;

        if (!keyspace.equals(that.keyspace) || !name.equals(that.name) || !fieldNames.equals(that.fieldNames))
            return false;

        if (!ignoreFreezing && isMultiCell != that.isMultiCell)
            return false;

        if (this.types.size() != that.types.size())
            return false;

        Iterator<AbstractType<?>> otherTypeIter = that.types.iterator();
        for (AbstractType<?> type : types)
        {
            if (!type.equals(otherTypeIter.next(), ignoreFreezing))
                return false;
        }

        return true;
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.UserDefined.create(this);
    }

    @Override
    public boolean referencesUserType(String userTypeName)
    {
        return getNameAsString().equals(userTypeName) ||
               fieldTypes().stream().anyMatch(f -> f.referencesUserType(userTypeName));
    }

    @Override
    public boolean referencesDuration()
    {
        return fieldTypes().stream().anyMatch(f -> f.referencesDuration());
    }

    @Override
    public String toString()
    {
        return this.toString(false);
    }

    @Override
    public boolean isTuple()
    {
        return false;
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

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return serializer;
    }
}

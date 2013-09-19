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
package org.apache.cassandra.config;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Defined (and loaded) user types.
 *
 * In practice, because user types are global, we have only one instance of
 * this class that retrieve through the Schema class.
 */
public final class UTMetaData
{
    private final Map<ByteBuffer, UserType> userTypes = new HashMap<>();

    // Only for Schema. You should generally not create instance of this, but rather use
    // the global reference Schema.instance().userTypes;
    UTMetaData() {}

    public static UTMetaData fromSchema(UntypedResultSet rows)
    {
        UTMetaData m = new UTMetaData();
        for (UntypedResultSet.Row row : rows)
            m.addType(fromSchema(row));
        return m;
    }

    private static UserType fromSchema(UntypedResultSet.Row row)
    {
        try
        {
            ByteBuffer name = ByteBufferUtil.bytes(row.getString("type_name"));
            List<String> rawColumns = row.getList("column_names", UTF8Type.instance);
            List<String> rawTypes = row.getList("column_types", UTF8Type.instance);

            List<ByteBuffer> columns = new ArrayList<>(rawColumns.size());
            for (String rawColumn : rawColumns)
                columns.add(ByteBufferUtil.bytes(rawColumn));

            List<AbstractType<?>> types = new ArrayList<>(rawTypes.size());
            for (String rawType : rawTypes)
                types.add(TypeParser.parse(rawType));

            return new UserType(name, columns, types);
        }
        catch (RequestValidationException e)
        {
            // If it has been written in the schema, it should be valid
            throw new AssertionError();
        }
    }

    public static UTMetaData fromSchema(List<Row> rows)
    {
        UntypedResultSet result = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_USER_TYPES_CF, rows);
        return fromSchema(result);
    }

    public static RowMutation toSchema(UserType newType, long timestamp)
    {
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, newType.name);
        ColumnFamily cf = rm.addOrGet(SystemKeyspace.SCHEMA_USER_TYPES_CF);

        ColumnNameBuilder builder = CFMetaData.SchemaUserTypesCf.getColumnNameBuilder();
        UpdateParameters params = new UpdateParameters(CFMetaData.SchemaUserTypesCf, Collections.<ByteBuffer>emptyList(), timestamp, 0, null);

        List<ByteBuffer> columnTypes = new ArrayList<>(newType.types.size());
        for (AbstractType<?> type : newType.types)
            columnTypes.add(ByteBufferUtil.bytes(type.toString()));

        try
        {
            new Lists.Setter(new ColumnIdentifier("column_names", false), new Lists.Value(newType.columnNames)).execute(newType.name, cf, builder.copy(), params);
            new Lists.Setter(new ColumnIdentifier("column_types", false), new Lists.Value(columnTypes)).execute(newType.name, cf, builder, params);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError();
        }

        return rm;
    }

    public static RowMutation dropFromSchema(UserType droppedType, long timestamp)
    {
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, droppedType.name);
        rm.delete(SystemKeyspace.SCHEMA_USER_TYPES_CF, timestamp);
        return rm;
    }

    public void addAll(UTMetaData types)
    {
        for (UserType type : types.userTypes.values())
            addType(type);
    }

    public UserType getType(ColumnIdentifier typeName)
    {
        return getType(typeName.bytes);
    }

    public UserType getType(ByteBuffer typeName)
    {
        return userTypes.get(typeName);
    }

    public Map<ByteBuffer, UserType> getAllTypes()
    {
        // Copy to avoid concurrent modification while iterating. Not intended to be called on a criticial path anyway
        return new HashMap<>(userTypes);
    }

    // This is *not* thread safe. As far as the global instance is concerned, only
    // Schema.loadType() (which is only called in DefsTables that is synchronized)
    // should use this.
    public void addType(UserType type)
    {
        UserType old = userTypes.get(type.name);
        assert old == null || type.isCompatibleWith(old);
        userTypes.put(type.name, type);
    }

    // Same remarks than for addType
    public void removeType(UserType type)
    {
        userTypes.remove(type.name);
    }
}

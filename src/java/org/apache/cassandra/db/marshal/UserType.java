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
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * A user defined type.
 */
public class UserType extends AbstractType<ByteBuffer>
{
    public final String keyspace;
    public final ByteBuffer name;
    public final List<ByteBuffer> fieldNames;
    public final List<AbstractType<?>> fieldTypes;

    public UserType(String keyspace, ByteBuffer name, List<ByteBuffer> fieldNames, List<AbstractType<?>> fieldTypes)
    {
        assert fieldNames.size() == fieldTypes.size();
        this.keyspace = keyspace;
        this.name = name;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public static UserType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> params = parser.getUserTypeParameters();
        String keyspace = params.left.left;
        ByteBuffer name = params.left.right;
        List<ByteBuffer> columnNames = new ArrayList<>(params.right.size());
        List<AbstractType<?>> columnTypes = new ArrayList<>(params.right.size());
        for (Pair<ByteBuffer, AbstractType> p : params.right)
        {
            columnNames.add(p.left);
            columnTypes.add(p.right);
        }
        return new UserType(keyspace, name, columnNames, columnTypes);
    }

    public String getNameAsString()
    {
        return UTF8Type.instance.compose(name);
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        int i = 0;
        while (bb1.remaining() > 0 && bb2.remaining() > 0)
        {
            AbstractType<?> comparator = fieldTypes.get(i);

            int size1 = bb1.getInt();
            int size2 = bb2.getInt();

            // Handle nulls
            if (size1 < 0)
            {
                if (size2 < 0)
                    continue;
                return -1;
            }
            if (size2 < 0)
                return 1;

            ByteBuffer value1 = ByteBufferUtil.readBytes(bb1, size1);
            ByteBuffer value2 = ByteBufferUtil.readBytes(bb2, size2);
            int cmp = comparator.compare(value1, value2);
            if (cmp != 0)
                return cmp;

            ++i;
        }

        if (bb1.remaining() == 0)
            return bb2.remaining() == 0 ? 0 : -1;

        // bb1.remaining() > 0 && bb2.remaining() == 0
        return 1;
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        ByteBuffer input = bytes.duplicate();
        for (int i = 0; i < fieldTypes.size(); i++)
        {
            // we allow the input to have less fields than declared so as to support field addition.
            if (!input.hasRemaining())
                return;

            if (input.remaining() < 4)
                throw new MarshalException(String.format("Not enough bytes to read size of %dth field %s", i, fieldNames.get(i)));

            int size = input.getInt();

            // size < 0 means null value
            if (size < 0)
                continue;

            if (input.remaining() < size)
                throw new MarshalException(String.format("Not enough bytes to read %dth field %s", i, fieldNames.get(i)));

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);
            fieldTypes.get(i).validate(field);
        }

        // We're allowed to get less fields than declared, but not more
        if (input.hasRemaining())
            throw new MarshalException("Invalid remaining data after end of UDT value");
    }

    /**
     * Split a UDT value into its fields values.
     */
    public ByteBuffer[] split(ByteBuffer value)
    {
        ByteBuffer[] fields = new ByteBuffer[fieldTypes.size()];
        ByteBuffer input = value.duplicate();
        for (int i = 0; i < fieldTypes.size(); i++)
        {
            if (!input.hasRemaining())
                return Arrays.copyOfRange(fields, 0, i);

            int size = input.getInt();
            fields[i] = size < 0 ? null : ByteBufferUtil.readBytes(input, size);
        }
        return fields;
    }

    public static ByteBuffer buildValue(ByteBuffer[] fields)
    {
        int totalLength = 0;
        for (ByteBuffer field : fields)
            totalLength += 4 + (field == null ? 0 : field.remaining());

        ByteBuffer result = ByteBuffer.allocate(totalLength);
        for (ByteBuffer field : fields)
        {
            if (field == null)
            {
                result.putInt(-1);
            }
            else
            {
                result.putInt(field.remaining());
                result.put(field.duplicate());
            }
        }
        result.rewind();
        return result;
    }

    @Override
    public String getString(ByteBuffer value)
    {
        StringBuilder sb = new StringBuilder();
        ByteBuffer input = value.duplicate();
        for (int i = 0; i < fieldTypes.size(); i++)
        {
            if (!input.hasRemaining())
                return sb.toString();

            if (i > 0)
                sb.append(":");

            AbstractType<?> type = fieldTypes.get(i);
            int size = input.getInt();
            if (size < 0)
            {
                sb.append("@");
                continue;
            }

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);
            // We use ':' as delimiter, and @ to represent null, so escape them in the generated string
            sb.append(type.getString(field).replaceAll(":", "\\\\:").replaceAll("@", "\\\\@"));
        }
        return sb.toString();
    }

    public ByteBuffer fromString(String source)
    {
        // Split the input on non-escaped ':' characters
        List<String> fieldStrings = AbstractCompositeType.split(source);
        ByteBuffer[] fields = new ByteBuffer[fieldStrings.size()];
        for (int i = 0; i < fieldStrings.size(); i++)
        {
            String fieldString = fieldStrings.get(i);
            // We use @ to represent nulls
            if (fieldString.equals("@"))
                continue;

            AbstractType<?> type = fieldTypes.get(i);
            fields[i] = type.fromString(fieldString.replaceAll("\\\\:", ":").replaceAll("\\\\@", "@"));
        }
        return buildValue(fields);
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(keyspace, name, fieldNames, fieldTypes);
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof UserType))
            return false;

        UserType that = (UserType)o;
        return keyspace.equals(that.keyspace) && name.equals(that.name) && fieldNames.equals(that.fieldNames) && fieldTypes.equals(that.fieldTypes);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.UserDefined.create(this);
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyUserTypeParameters(keyspace, name, fieldNames, fieldTypes);
    }
}

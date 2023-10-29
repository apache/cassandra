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
package org.apache.cassandra.cql3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public enum Operator
{
    EQ(0)
    {
        @Override
        public String toString()
        {
            return "=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) == 0;
        }
    },
    LT(4)
    {
        @Override
        public String toString()
        {
            return "<";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) < 0;
        }
    },
    LTE(3)
    {
        @Override
        public String toString()
        {
            return "<=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) <= 0;
        }
    },
    GTE(1)
    {
        @Override
        public String toString()
        {
            return ">=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) >= 0;
        }
    },
    GT(2)
    {
        @Override
        public String toString()
        {
            return ">";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) > 0;
        }
    },
    IN(7)
    {
        @Override
        public String toString()
        {
            return "IN";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            ListSerializer<?> serializer = ListType.getInstance(type, false).getSerializer();
            return serializer.anyMatch(rightOperand, r -> type.compareForCQL(leftOperand, r) == 0);
        }
    },
    CONTAINS(5)
    {
        @Override
        public String toString()
        {
            return "CONTAINS";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            switch(((CollectionType<?>) type).kind)
            {
                case LIST :
                    ListType<?> listType = (ListType<?>) type;
                    List<?> list = listType.getSerializer().deserialize(leftOperand);
                    return list.contains(listType.getElementsType().getSerializer().deserialize(rightOperand));
                case SET:
                    SetType<?> setType = (SetType<?>) type;
                    Set<?> set = setType.getSerializer().deserialize(leftOperand);
                    return set.contains(setType.getElementsType().getSerializer().deserialize(rightOperand));
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) type;
                    Map<?, ?> map = mapType.getSerializer().deserialize(leftOperand);
                    return map.containsValue(mapType.getValuesType().getSerializer().deserialize(rightOperand));
                default:
                    throw new AssertionError();
            }
        }
    },
    CONTAINS_KEY(6)
    {
        @Override
        public String toString()
        {
            return "CONTAINS KEY";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;
            Map<?, ?> map = mapType.getSerializer().deserialize(leftOperand);
            return map.containsKey(mapType.getKeysType().getSerializer().deserialize(rightOperand));
        }
    },
    NEQ(8)
    {
        @Override
        public String toString()
        {
            return "!=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) != 0;

        }
    },
    IS_NOT(9)
    {
        @Override
        public String toString()
        {
            return "IS NOT";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            throw new UnsupportedOperationException();
        }
    },
    LIKE_PREFIX(10)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.startsWith(leftOperand, rightOperand);
        }
    },
    LIKE_SUFFIX(11)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.endsWith(leftOperand, rightOperand);
        }
    },
    LIKE_CONTAINS(12)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.contains(leftOperand, rightOperand);
        }
    },
    LIKE_MATCHES(13)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>'";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.contains(leftOperand, rightOperand);
        }
    },
    LIKE(14)
    {
        @Override
        public String toString()
        {
            return "LIKE";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            throw new UnsupportedOperationException();
        }
    },
    ANN(15)
    {
        @Override
        public String toString()
        {
            return "ANN";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            // The ANN operator is only supported by the vector index so, normally, should never be called directly.
            // In networked queries (non-local) the coordinator will end up calling the row filter directly. So, this
            // needs to return true so that the returned values are allowed through to the VectorTopKProcessor
            return true;
        }
    };

    /**
     * The binary representation of this <code>Enum</code> value.
     */
    private final int b;

    /**
     * Creates a new <code>Operator</code> with the specified binary representation.
     * @param b the binary representation of this <code>Enum</code> value
     */
    private Operator(int b)
    {
        this.b = b;
    }

    /**
     * Write the serialized version of this <code>Operator</code> to the specified output.
     *
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs while writing to the specified output
     */
    public void writeTo(DataOutput output) throws IOException
    {
        output.writeInt(b);
    }

    public int getValue()
    {
        return b;
    }

    /**
     * Deserializes a <code>Operator</code> instance from the specified input.
     *
     * @param input the input to read from
     * @return the <code>Operator</code> instance deserialized
     * @throws IOException if a problem occurs while deserializing the <code>Type</code> instance.
     */
    public static Operator readFrom(DataInput input) throws IOException
    {
          int b = input.readInt();
          for (Operator operator : values())
              if (operator.b == b)
                  return operator;

          throw new IOException(String.format("Cannot resolve Relation.Type from binary representation: %s", b));
    }

    /**
     * Whether 2 values satisfy this operator (given the type they should be compared with).
     */
    public abstract boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand);

    public int serializedSize()
    {
        return 4;
    }

    /**
     * Checks if this operator is a slice operator.
     * @return {@code true} if this operator is a slice operator, {@code false} otherwise.
     */
    public boolean isSlice()
    {
        return this == LT || this == LTE || this == GT || this == GTE;
    }

    @Override
    public String toString()
    {
         return this.name();
    }

    /**
     * Checks if this operator is an IN operator.
     * @return {@code true} if this operator is an IN operator, {@code false} otherwise.
     */
    public boolean isIN()
    {
        return this == IN;
    }

    /**
     * Checks if this operator is CONTAINS operator.
     * @return {@code true} if this operator is a CONTAINS operator, {@code false} otherwise.
     */
    public boolean isContains()
    {
        return this == CONTAINS;
    }

    /**
     * Checks if this operator is CONTAINS KEY operator.
     * @return {@code true} if this operator is a CONTAINS operator, {@code false} otherwise.
     */
    public boolean isContainsKey()
    {
        return this == CONTAINS_KEY;
    }
}

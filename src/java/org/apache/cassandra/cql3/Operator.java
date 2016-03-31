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
    },
    LT(4)
    {
        @Override
        public String toString()
        {
            return "<";
        }
    },
    LTE(3)
    {
        @Override
        public String toString()
        {
            return "<=";
        }
    },
    GTE(1)
    {
        @Override
        public String toString()
        {
            return ">=";
        }
    },
    GT(2)
    {
        @Override
        public String toString()
        {
            return ">";
        }
    },
    IN(7)
    {
    },
    CONTAINS(5)
    {
    },
    CONTAINS_KEY(6)
    {
        @Override
        public String toString()
        {
            return "CONTAINS KEY";
        }
    },
    NEQ(8)
    {
        @Override
        public String toString()
        {
            return "!=";
        }
    },
    IS_NOT(9)
    {
        @Override
        public String toString()
        {
            return "IS NOT";
        }
    },
    LIKE_PREFIX(10)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>%'";
        }
    },
    LIKE_SUFFIX(11)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>'";
        }
    },
    LIKE_CONTAINS(12)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>%'";
        }
    },
    LIKE_MATCHES(13)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>'";
        }
    },
    LIKE(14)
    {
        @Override
        public String toString()
        {
            return "LIKE";
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
     *
     * @throws AssertionError for CONTAINS and CONTAINS_KEY as this doesn't support those operators yet
     */
    public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
    {
        switch (this)
        {
            case EQ:
                return type.compareForCQL(leftOperand, rightOperand) == 0;
            case LT:
                return type.compareForCQL(leftOperand, rightOperand) < 0;
            case LTE:
                return type.compareForCQL(leftOperand, rightOperand) <= 0;
            case GT:
                return type.compareForCQL(leftOperand, rightOperand) > 0;
            case GTE:
                return type.compareForCQL(leftOperand, rightOperand) >= 0;
            case NEQ:
                return type.compareForCQL(leftOperand, rightOperand) != 0;
            case IN:
                List inValues = ((List) ListType.getInstance(type, false).getSerializer().deserialize(rightOperand));
                return inValues.contains(type.getSerializer().deserialize(leftOperand));
            case CONTAINS:
                if (type instanceof ListType)
                {
                    List list = (List) type.getSerializer().deserialize(leftOperand);
                    return list.contains(((ListType) type).getElementsType().getSerializer().deserialize(rightOperand));
                }
                else if (type instanceof SetType)
                {
                    Set set = (Set) type.getSerializer().deserialize(leftOperand);
                    return set.contains(((SetType) type).getElementsType().getSerializer().deserialize(rightOperand));
                }
                else  // MapType
                {
                    Map map = (Map) type.getSerializer().deserialize(leftOperand);
                    return map.containsValue(((MapType) type).getValuesType().getSerializer().deserialize(rightOperand));
                }
            case CONTAINS_KEY:
                Map map = (Map) type.getSerializer().deserialize(leftOperand);
                return map.containsKey(((MapType) type).getKeysType().getSerializer().deserialize(rightOperand));
            case LIKE_PREFIX:
                return ByteBufferUtil.startsWith(leftOperand, rightOperand);
            case LIKE_SUFFIX:
                return ByteBufferUtil.endsWith(leftOperand, rightOperand);
            case LIKE_MATCHES:
            case LIKE_CONTAINS:
                return ByteBufferUtil.contains(leftOperand, rightOperand);
            default:
                // we shouldn't get LIKE, CONTAINS, CONTAINS KEY, or IS NOT here
                throw new AssertionError();
        }
    }

    public int serializedSize()
    {
        return 4;
    }

    @Override
    public String toString()
    {
         return this.name();
    }
}

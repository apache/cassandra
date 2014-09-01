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

import org.apache.cassandra.db.marshal.AbstractType;

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
     * @throws AssertionError for IN, CONTAINS and CONTAINS_KEY as this doesn't make sense for this function.
     */
    public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
    {
        int comparison = type.compareForCQL(leftOperand, rightOperand);
        switch (this)
        {
            case EQ:
                return comparison == 0;
            case LT:
                return comparison < 0;
            case LTE:
                return comparison <= 0;
            case GT:
                return comparison > 0;
            case GTE:
                return comparison >= 0;
            case NEQ:
                return comparison != 0;
            default:
                // we shouldn't get IN, CONTAINS, or CONTAINS KEY here
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

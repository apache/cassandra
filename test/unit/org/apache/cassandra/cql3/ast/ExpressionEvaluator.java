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

package org.apache.cassandra.cql3.ast;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.cassandra.db.marshal.AbstractType;

import static java.util.Optional.of;

public class ExpressionEvaluator
{
    public static Optional<Object> tryEval(Expression e)
    {
        if (e instanceof Value)
            return of(((Value) e).value());
        if (e instanceof TypeHint)
            return tryEval(((TypeHint) e).e);
        if (e instanceof Operator)
            return tryEval((Operator) e);
        return Optional.empty();
    }

    public static Optional<Object> tryEval(Operator e)
    {
        switch (e.kind)
        {
            case ADD:
            {
                var lhsOpt = tryEval(e.left);
                var rhsOpt = tryEval(e.right);
                if (lhsOpt.isEmpty() || rhsOpt.isEmpty())
                    return Optional.empty();
                Object lhs = lhsOpt.get();
                Object rhs = rhsOpt.get();
                if (lhs instanceof Byte)
                    return of((byte) (((Byte) lhs) + ((Byte) rhs)));
                if (lhs instanceof Short)
                    return of((short) (((Short) lhs) + ((Short) rhs)));
                if (lhs instanceof Integer)
                    return of((int) (((Integer) lhs) + ((Integer) rhs)));
                if (lhs instanceof Long)
                    return of((long) (((Long) lhs) + ((Long) rhs)));
                if (lhs instanceof Float)
                    return of((float) (((Float) lhs) + ((Float) rhs)));
                if (lhs instanceof Double)
                    return of((double) (((Double) lhs) + ((Double) rhs)));
                if (lhs instanceof BigInteger)
                    return of(((BigInteger) lhs).add((BigInteger) rhs));
                if (lhs instanceof BigDecimal)
                    return of(((BigDecimal) lhs).add((BigDecimal) rhs));
                if (lhs instanceof String)
                    return of(lhs.toString() + rhs.toString());
                throw new UnsupportedOperationException("Unexpected type: " + lhs.getClass());
            }
            case SUBTRACT:
            {
                var lhsOpt = tryEval(e.left);
                var rhsOpt = tryEval(e.right);
                if (lhsOpt.isEmpty() || rhsOpt.isEmpty())
                    return Optional.empty();
                Object lhs = lhsOpt.get();
                Object rhs = rhsOpt.get();
                if (lhs instanceof Byte)
                    return of((byte) (((Byte) lhs) - ((Byte) rhs)));
                if (lhs instanceof Short)
                    return of((short) (((Short) lhs) - ((Short) rhs)));
                if (lhs instanceof Integer)
                    return of((int) (((Integer) lhs) - ((Integer) rhs)));
                if (lhs instanceof Long)
                    return of((long) (((Long) lhs) - ((Long) rhs)));
                if (lhs instanceof Float)
                    return of((float) (((Float) lhs) - ((Float) rhs)));
                if (lhs instanceof Double)
                    return of((double) (((Double) lhs) - ((Double) rhs)));
                if (lhs instanceof BigInteger)
                    return of(((BigInteger) lhs).subtract((BigInteger) rhs));
                if (lhs instanceof BigDecimal)
                    return of(((BigDecimal) lhs).subtract((BigDecimal) rhs));
                throw new UnsupportedOperationException("Unexpected type: " + lhs.getClass());
            }
            case MULTIPLY:
            {
                var lhsOpt = tryEval(e.left);
                var rhsOpt = tryEval(e.right);
                if (lhsOpt.isEmpty() || rhsOpt.isEmpty())
                    return Optional.empty();
                Object lhs = lhsOpt.get();
                Object rhs = rhsOpt.get();
                if (lhs instanceof Byte)
                    return of((byte) (((Byte) lhs) * ((Byte) rhs)));
                if (lhs instanceof Short)
                    return of((short) (((Short) lhs) * ((Short) rhs)));
                if (lhs instanceof Integer)
                    return of((int) (((Integer) lhs) * ((Integer) rhs)));
                if (lhs instanceof Long)
                    return of((long) (((Long) lhs) * ((Long) rhs)));
                if (lhs instanceof Float)
                    return of((float) (((Float) lhs) * ((Float) rhs)));
                if (lhs instanceof Double)
                    return of((double) ((Double) lhs) * ((Double) rhs));
                if (lhs instanceof BigInteger)
                    return of(((BigInteger) lhs).multiply((BigInteger) rhs));
                if (lhs instanceof BigDecimal)
                    return of(((BigDecimal) lhs).multiply((BigDecimal) rhs));
                throw new UnsupportedOperationException("Unexpected type: " + lhs.getClass());
            }
            default:
                throw new UnsupportedOperationException(e.kind.name());
        }
    }

    public static Optional<ByteBuffer> tryEvalEncoded(Expression e)
    {
        return tryEval(e).map(v -> {
            try
            {
                return ((AbstractType) e.type()).decompose(v);
            }
            catch (Throwable t)
            {
                throw t;
            }
        });
    }
}

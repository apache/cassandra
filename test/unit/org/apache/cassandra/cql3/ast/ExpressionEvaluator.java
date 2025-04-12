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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.AbstractType;

public class ExpressionEvaluator
{
    @Nullable
    public static Object eval(Expression e)
    {
        if (e instanceof Value)
            return ((Value) e).value();
        if (e instanceof TypeHint)
            return eval(((TypeHint) e).e);
        if (e instanceof Operator)
            return eval((Operator) e);
        throw new UnsupportedOperationException("Unexpected expression type " + e.getClass() + ": " + e.toCQL());
    }

    @Nullable
    public static Object eval(Operator e)
    {
        Object lhs = eval(e.left);
        if (lhs instanceof ByteBuffer)
            lhs = e.left.type().compose((ByteBuffer) lhs);
        Object rhs = eval(e.right);
        if (rhs instanceof ByteBuffer)
            rhs = e.right.type().compose((ByteBuffer) rhs);
        // null + 42 = null
        // 42 + null = null
        // if anything is null, everything is null!
        if (lhs == null || rhs == null)
            return null;
        switch (e.kind)
        {
            case ADD:
            {
                if (lhs instanceof Byte)
                    return (byte) (((Byte) lhs) + ((Byte) rhs));
                if (lhs instanceof Short)
                    return (short) (((Short) lhs) + ((Short) rhs));
                if (lhs instanceof Integer)
                    return (int) (((Integer) lhs) + ((Integer) rhs));
                if (lhs instanceof Long)
                    return (long) (((Long) lhs) + ((Long) rhs));
                if (lhs instanceof Float)
                    return (float) (((Float) lhs) + ((Float) rhs));
                if (lhs instanceof Double)
                    return (double) (((Double) lhs) + ((Double) rhs));
                if (lhs instanceof BigInteger)
                    return ((BigInteger) lhs).add((BigInteger) rhs);
                if (lhs instanceof BigDecimal)
                    return ((BigDecimal) lhs).add((BigDecimal) rhs);
                if (lhs instanceof String)
                    return lhs.toString() + rhs.toString();
                if (lhs instanceof Set)
                {
                    Set<Object> accum = new HashSet<>((Set<Object>) lhs);
                    accum.addAll((Set<Object>) rhs);
                    return accum;
                }
                if (lhs instanceof List)
                {
                    List<Object> accum = new ArrayList<>((List<Object>) lhs);
                    accum.addAll((List<Object>) rhs);
                    return accum;
                }
                if (lhs instanceof Map)
                {
                    Map<Object, Object> accum = new HashMap<>((Map<Object, Object>) lhs);
                    accum.putAll((Map<Object, Object>) rhs);
                    return accum;
                }
                throw new UnsupportedOperationException("Unexpected type: " + lhs.getClass());
            }
            case SUBTRACT:
            {
                if (lhs instanceof Byte)
                    return (byte) (((Byte) lhs) - ((Byte) rhs));
                if (lhs instanceof Short)
                    return (short) (((Short) lhs) - ((Short) rhs));
                if (lhs instanceof Integer)
                    return (int) (((Integer) lhs) - ((Integer) rhs));
                if (lhs instanceof Long)
                    return (long) (((Long) lhs) - ((Long) rhs));
                if (lhs instanceof Float)
                    return (float) (((Float) lhs) - ((Float) rhs));
                if (lhs instanceof Double)
                    return (double) (((Double) lhs) - ((Double) rhs));
                if (lhs instanceof BigInteger)
                    return ((BigInteger) lhs).subtract((BigInteger) rhs);
                if (lhs instanceof BigDecimal)
                    return ((BigDecimal) lhs).subtract((BigDecimal) rhs);
                if (lhs instanceof Set)
                {
                    Set<Object> accum = new HashSet<>((Set<Object>) lhs);
                    accum.removeAll((Set<Object>) rhs);
                    return accum.isEmpty() ? null : accum;
                }
                if (lhs instanceof List)
                {
                    List<Object> accum = new ArrayList<>((List<Object>) lhs);
                    accum.removeAll((List<Object>) rhs);
                    return accum.isEmpty() ? null : accum;
                }
                if (lhs instanceof Map)
                {
                    // rhs is a Set<Object> as CQL doesn't allow removing if the key and value both match
                    Map<Object, Object> accum = new HashMap<>((Map<Object, Object>) lhs);
                    ((Set<Object>) rhs).forEach(accum::remove);
                    return accum.isEmpty() ? null : accum;
                }
                throw new UnsupportedOperationException("Unexpected type: " + lhs.getClass());
            }
            case MULTIPLY:
            {
                if (lhs instanceof Byte)
                    return (byte) (((Byte) lhs) * ((Byte) rhs));
                if (lhs instanceof Short)
                    return (short) (((Short) lhs) * ((Short) rhs));
                if (lhs instanceof Integer)
                    return (int) (((Integer) lhs) * ((Integer) rhs));
                if (lhs instanceof Long)
                    return (long) (((Long) lhs) * ((Long) rhs));
                if (lhs instanceof Float)
                    return (float) (((Float) lhs) * ((Float) rhs));
                if (lhs instanceof Double)
                    return (double) ((Double) lhs) * ((Double) rhs);
                if (lhs instanceof BigInteger)
                    return ((BigInteger) lhs).multiply((BigInteger) rhs);
                if (lhs instanceof BigDecimal)
                    return ((BigDecimal) lhs).multiply((BigDecimal) rhs);
                throw new UnsupportedOperationException("Unexpected type: " + lhs.getClass());
            }
            default:
                throw new UnsupportedOperationException(e.kind.name());
        }
    }

    @Nullable
    public static ByteBuffer evalEncoded(Expression e)
    {
        Object v = eval(e);
        if (v == null) return null;
        if (v instanceof ByteBuffer) return (ByteBuffer) v;
        return ((AbstractType) e.type()).decompose(v);
    }
}

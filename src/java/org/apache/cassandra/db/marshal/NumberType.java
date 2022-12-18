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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Base type for the numeric types.
 */
public abstract class NumberType<T extends Number> extends AbstractType<T>
{
    protected NumberType(ComparisonType comparisonType)
    {
        super(comparisonType);
    }

    /**
     * Checks if this type support floating point numbers.
     * @return {@code true} if this type support floating point numbers, {@code false} otherwise.
     */
    public boolean isFloatingPoint()
    {
        return false;
    }

    /**
     * Converts the specified value into a <code>BigInteger</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected BigInteger toBigInteger(ByteBuffer value)
    {
        return BigInteger.valueOf(toLong(value));
    }

    /**
     * Converts the specified value into a <code>BigDecimal</code>.
     *
     * @param value the value to convert
     * @return the converted value
     */
    protected BigDecimal toBigDecimal(ByteBuffer value)
    {
        double d = toDouble(value);

        if (Double.isNaN(d))
            throw new NumberFormatException("A NaN cannot be converted into a decimal");

        if (Double.isInfinite(d))
            throw new NumberFormatException("An infinite number cannot be converted into a decimal");

        return BigDecimal.valueOf(d);
    }

    /**
     * Converts the specified value into a <code>byte</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected byte toByte(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts the specified value into a <code>short</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected short toShort(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts the specified value into an <code>int</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected int toInt(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts the specified value into a <code>long</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected long toLong(ByteBuffer value)
    {
        return toInt(value);
    }

    /**
     * Converts the specified value into a <code>float</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected float toFloat(ByteBuffer value)
    {
        return toInt(value);
    }

    /**
     * Converts the specified value into a <code>double</code> if allowed.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws UnsupportedOperationException if the value cannot be converted without losing precision
     */
    protected double toDouble(ByteBuffer value)
    {
        return toLong(value);
    }

    /**
     * Adds the left argument to the right one.
     *
     * @param leftType the type associated to the left argument
     * @param left the left argument
     * @param rightType the type associated to the right argument
     * @param right the right argument
     * @return the addition result
     */
    public abstract ByteBuffer add(NumberType<?> leftType,
                                   ByteBuffer left,
                                   NumberType<?> rightType,
                                   ByteBuffer right);

    /**
     * Substracts the left argument from the right one.
     *
     * @param leftType the type associated to the left argument
     * @param left the left argument
     * @param rightType the type associated to the right argument
     * @param right the right argument
     * @return the substraction result
     */
    public abstract ByteBuffer substract(NumberType<?> leftType,
                                         ByteBuffer left,
                                         NumberType<?> rightType,
                                         ByteBuffer right);

    /**
     * Multiplies the left argument with the right one.
     *
     * @param leftType the type associated to the left argument
     * @param left the left argument
     * @param rightType the type associated to the right argument
     * @param right the right argument
     * @return the multiplication result
     */
    public abstract ByteBuffer multiply(NumberType<?> leftType,
                                        ByteBuffer left,
                                        NumberType<?> rightType,
                                        ByteBuffer right);

    /**
     * Divides the left argument by the right one.
     *
     * @param leftType the type associated to the left argument
     * @param left the left argument
     * @param rightType the type associated to the right argument
     * @param right the right argument
     * @return the division result
     */
    public abstract ByteBuffer divide(NumberType<?> leftType,
                                      ByteBuffer left,
                                      NumberType<?> rightType,
                                      ByteBuffer right);

    /**
     * Return the remainder.
     *
     * @param leftType the type associated to the left argument
     * @param left the left argument
     * @param rightType the type associated to the right argument
     * @param right the right argument
     * @return the remainder
     */
    public abstract ByteBuffer mod(NumberType<?> leftType,
                                   ByteBuffer left,
                                   NumberType<?> rightType,
                                   ByteBuffer right);

    /**
     * Negates the argument.
     *
     * @param input the argument to negate
     * @return the negated argument
     */
    public abstract ByteBuffer negate(ByteBuffer input);

    /**
     * Takes the absolute value of the argument.
     *
     * @param input the argument to take the absolute value of
     * @return a ByteBuffer containing the absolute value of the argument. The type of the contents of the ByteBuffer
     * will match that of the input.
     */
    public abstract ByteBuffer abs(ByteBuffer input);

    /**
     * Raises e to the power of the argument.
     *
     * @param input the argument to raise e to
     * @return a ByteBuffer containg the value of e (the base of natural logarithms) raised to the power of the
     * argument. The type of the contents of the ByteBuffer will be a double, unless the input is a DecimalType or
     * IntegerType, in which the ByteBuffer will contain a DecimalType.
     */
    public abstract ByteBuffer exp(ByteBuffer input);

    /**
     * Takes the natural logrithm of the argument.
     *
     * @param input the argument to take the natural log (ln) of
     * @return a ByteBuffer containg the log base e (ln) of the argument. The type of the contents of the ByteBuffer
     * will be a double, unless the input is a DecimalType or IntegerType, in which the ByteBuffer will contain a
     * DecimalType.
     */
    public abstract ByteBuffer log(ByteBuffer input);

    /**
     * Takes the log base 10 of the arguement.
     *
     * @param input the argument to take the log base ten of
     * @return a ByteBuffer containg the log base 10 of the argument. The type of the contents of the ByteBuffer
     * will be a double, unless the input is a DecimalType or IntegerType, in which the ByteBuffer will contain a
     * DecimalType.
     */
    public abstract ByteBuffer log10(ByteBuffer input);

    /**
     * Rounds the argument to the nearest whole number.
     *
     * @param input the argument to round
     * @return a ByteBuffer containg the rounded argument. If the input is an integral type, the ByteBuffer will contain
     * a copy of the input. If the input is a float, the ByteBuffer will contain an int32. If the input is a double, the
     * output will contain a long. If the input is a DecimalType, the output will contain an IntegerType.
     */
    public abstract ByteBuffer round(ByteBuffer input);
}

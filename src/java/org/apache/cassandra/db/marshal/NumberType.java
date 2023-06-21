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

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.transport.ProtocolVersion;

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
     * Adds the left argument to the right one.
     *
     * @param left the left argument
     * @param right the right argument
     * @return the addition result
     */
    public abstract ByteBuffer add(Number left, Number right);

    /**
     * Substracts the left argument from the right one.
     *
     * @param left the left argument
     * @param right the right argument
     * @return the substraction result
     */
    public abstract ByteBuffer substract(Number left, Number right);

    /**
     * Multiplies the left argument with the right one.
     *
     * @param left the left argument
     * @param right the right argument
     * @return the multiplication result
     */
    public abstract ByteBuffer multiply(Number left, Number right);

    /**
     * Divides the left argument by the right one.
     *
     * @param left the left argument
     * @param right the right argument
     * @return the division result
     */
    public abstract ByteBuffer divide(Number left, Number right);

    /**
     * Return the remainder.
     *
     * @param left the left argument
     * @param right the right argument
     * @return the remainder
     */
    public abstract ByteBuffer mod(Number left, Number right);

    /**
     * Negates the argument.
     *
     * @param input the argument to negate
     * @return the negated argument
     */
    public abstract ByteBuffer negate(Number input);

    /**
     * Takes the absolute value of the argument.
     *
     * @param input the argument to take the absolute value of
     * @return a ByteBuffer containing the absolute value of the argument. The type of the contents of the ByteBuffer
     * will match that of the input.
     */
    public abstract ByteBuffer abs(Number input);

    /**
     * Raises e to the power of the argument.
     *
     * @param input the argument to raise e to
     * @return a ByteBuffer containg the value of e (the base of natural logarithms) raised to the power of the
     * argument. The type of the contents of the ByteBuffer will be a double, unless the input is a DecimalType or
     * IntegerType, in which the ByteBuffer will contain a DecimalType.
     */
    public abstract ByteBuffer exp(Number input);

    /**
     * Takes the natural logrithm of the argument.
     *
     * @param input the argument to take the natural log (ln) of
     * @return a ByteBuffer containg the log base e (ln) of the argument. The type of the contents of the ByteBuffer
     * will be a double, unless the input is a DecimalType or IntegerType, in which the ByteBuffer will contain a
     * DecimalType.
     */
    public abstract ByteBuffer log(Number input);

    /**
     * Takes the log base 10 of the arguement.
     *
     * @param input the argument to take the log base ten of
     * @return a ByteBuffer containg the log base 10 of the argument. The type of the contents of the ByteBuffer
     * will be a double, unless the input is a DecimalType or IntegerType, in which the ByteBuffer will contain a
     * DecimalType.
     */
    public abstract ByteBuffer log10(Number input);

    /**
     * Rounds the argument to the nearest whole number.
     *
     * @param input the argument to round
     * @return a ByteBuffer containg the rounded argument. If the input is an integral type, the ByteBuffer will contain
     * a copy of the input. If the input is a float, the ByteBuffer will contain an int32. If the input is a double, the
     * output will contain a long. If the input is a DecimalType, the output will contain an IntegerType.
     */
    public abstract ByteBuffer round(Number input);

    /**
     * Base class for numeric type {@link ArgumentDeserializer}.
     *
     * <p>This class uses and returns a mutable wrapper instead of the Java immutable primitive wrapper. This wrapper
     * is being reused between each call to minimize the amount of objects instantiated.</p>
     *
     * @param <M> The Mutable wrapper type
     */
    protected abstract static class NumberArgumentDeserializer<M extends Mutable<Number>> implements ArgumentDeserializer
    {
        protected final M wrapper;

        public NumberArgumentDeserializer(M wrapper)
        {
            this.wrapper = wrapper;
        }

        @Override
        public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer)
        {
            if (buffer == null || !buffer.hasRemaining())
                return null;

            setMutableValue(wrapper, buffer);
            return wrapper;
        }

        /**
         * Sets the value of the mutable.
         * @param buffer the serialized value.
         */
        protected abstract void setMutableValue(M mutable, ByteBuffer buffer);
    }
}

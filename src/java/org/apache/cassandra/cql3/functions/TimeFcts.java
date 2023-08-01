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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class TimeFcts
{
    public static Logger logger = LoggerFactory.getLogger(TimeFcts.class);

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.addAll(new NowFunction("now", TimeUUIDType.instance),
                         new NowFunction("current_timeuuid", TimeUUIDType.instance),
                         new NowFunction("current_timestamp", TimestampType.instance),
                         new NowFunction("current_date", SimpleDateType.instance),
                         new NowFunction("current_time", TimeType.instance),
                         minTimeuuidFct,
                         maxTimeuuidFct,
                         toDate(TimeUUIDType.instance),
                         toTimestamp(TimeUUIDType.instance),
                         toUnixTimestamp(TimeUUIDType.instance),
                         toUnixTimestamp(TimestampType.instance),
                         toDate(TimestampType.instance),
                         toUnixTimestamp(SimpleDateType.instance),
                         toTimestamp(SimpleDateType.instance),
                         FloorTimestampFunction.newInstance(),
                         FloorTimestampFunction.newInstanceWithStartTimeArgument(),
                         FloorTimeUuidFunction.newInstance(),
                         FloorTimeUuidFunction.newInstanceWithStartTimeArgument(),
                         FloorDateFunction.newInstance(),
                         FloorDateFunction.newInstanceWithStartTimeArgument(),
                         floorTime);
    }

    private static class NowFunction extends NativeScalarFunction
    {
        private final TemporalType<?> type;

        public NowFunction(String name, TemporalType<?> type)
        {
            super(name, type);
            this.type = type;
        }

        @Override
        public ByteBuffer execute(Arguments arguments)
        {
            return type.now();
        }

        @Override
        public boolean isPure()
        {
            return false; // as it returns non-identical results for identical arguments
        }

        @Override
        public NativeFunction withLegacyName()
        {
            String name = name().name;
            return name.contains("current") ? new NowFunction(StringUtils.remove(name, '_'), type) : null;
        }
    }

    public static abstract class TemporalConversionFunction extends NativeScalarFunction
    {
        protected TemporalConversionFunction(String name, AbstractType<?> returnType, AbstractType<?>... argsType)
        {
            super(name, returnType, argsType);
        }

        public ByteBuffer execute(Arguments arguments)
        {
            beforeExecution();

            if (arguments.containsNulls())
                return null;

            return convertArgument(arguments.getAsLong(0));
        }

        protected void beforeExecution()
        {
        }

        protected abstract ByteBuffer convertArgument(long timeInMillis);
    }

    public static final NativeFunction minTimeuuidFct = new MinTimeuuidFunction(false);

    private static final class MinTimeuuidFunction extends TemporalConversionFunction
    {
        public MinTimeuuidFunction(boolean legacy)
        {
            super(legacy ? "mintimeuuid" : "min_timeuuid", TimeUUIDType.instance, TimestampType.instance);
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return TimeUUID.minAtUnixMillis(timeInMillis).toBytes();
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new MinTimeuuidFunction(true);
        }
    }

    public static final NativeFunction maxTimeuuidFct = new MaxTimeuuidFunction(false);

    private static final class MaxTimeuuidFunction extends TemporalConversionFunction
    {
        public MaxTimeuuidFunction(boolean legacy)
        {
            super(legacy ? "maxtimeuuid" : "max_timeuuid", TimeUUIDType.instance, TimestampType.instance);
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return TimeUUID.maxAtUnixMillis(timeInMillis).toBytes();
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new MaxTimeuuidFunction(true);
        }
    }

    /**
     * Creates a function that converts a value of the specified type into a {@code DATE}.
     *
     * @param type the temporal type
     * @return a function that convert a value of the specified type into a <code>DATE</code>.
     */
    public static NativeScalarFunction toDate(TemporalType<?> type)
    {
        return new ToDateFunction(type, false);
    }

    private static class ToDateFunction extends TemporalConversionFunction
    {
        private final TemporalType<?> type;

        public ToDateFunction(TemporalType<?> type, boolean useLegacyName)
        {
            super(useLegacyName ? "todate" : "to_date", SimpleDateType.instance, type);
            this.type = type;
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
        }

        @Override
        public boolean isMonotonic()
        {
            return true;
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new ToDateFunction(type, true);
        }
    }

    /**
     * Creates a function that converts a value of the specified type into a {@code TIMESTAMP}.
     *
     * @param type the temporal type
     * @return a function that convert a value of the specified type into a {@code TIMESTAMP}.
     */
    public static NativeScalarFunction toTimestamp(TemporalType<?> type)
    {
        return new ToTimestampFunction(type, false);
    }

    private static class ToTimestampFunction extends TemporalConversionFunction
    {
        private final TemporalType<?> type;

        public ToTimestampFunction(TemporalType<?> type, boolean useLegacyName)
        {
            super(useLegacyName ? "totimestamp" : "to_timestamp", TimestampType.instance, type);
            this.type = type;
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return TimestampType.instance.fromTimeInMillis(timeInMillis);
        }

        @Override
        public boolean isMonotonic()
        {
            return true;
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new ToTimestampFunction(type, true);
        }
    }

    /**
     * Creates a function that converts a value of the specified type into a UNIX timestamp.
     *
     * @param type the temporal type
     * @return a function that convert a value of the specified type into a UNIX timestamp.
     */
    public static NativeScalarFunction toUnixTimestamp(TemporalType<?> type)
    {
        return new ToUnixTimestampFunction(type, false);
    }

    private static class ToUnixTimestampFunction extends TemporalConversionFunction
    {
        private final TemporalType<?> type;

        private ToUnixTimestampFunction(TemporalType<?> type, boolean useLegacyName)
        {
            super(useLegacyName ? "tounixtimestamp" : "to_unix_timestamp", LongType.instance, type);
            this.type = type;
        }

        @Override
        protected ByteBuffer convertArgument(long timeInMillis)
        {
            return ByteBufferUtil.bytes(timeInMillis);
        }

        @Override
        public boolean isMonotonic()
        {
            return true;
        }

        @Override
        public NativeFunction withLegacyName()
        {
            return new ToUnixTimestampFunction(type, true);
        }
    }

    /**
     * Function that rounds a timestamp down to the closest multiple of a duration.
     */
     private static abstract class FloorFunction extends NativeScalarFunction
     {
         private static final Long ZERO = 0L;

         protected FloorFunction(AbstractType<?> returnType,
                                 AbstractType<?>... argsType)
         {
             super("floor", returnType, argsType);
             // The function can accept either 2 parameters (time and duration) or 3 parameters (time, duration and startTime)r
             assert argsType.length == 2 || argsType.length == 3;
         }

         @Override
         protected boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters)
         {
             return partialParameters.get(0) == UNRESOLVED
                     && partialParameters.get(1) != UNRESOLVED
                     && (partialParameters.size() == 2 || partialParameters.get(2) != UNRESOLVED);
         }

         @Override
         public final ByteBuffer execute(Arguments arguments)
         {
             if (arguments.containsNulls())
                 return null;

             long time = arguments.getAsLong(0);
             Duration duration = arguments.get(1);
             long startingTime = getStartingTime(arguments);
             validateDuration(duration);

             long floor = Duration.floorTimestamp(time, duration, startingTime);

             return fromTimeInMillis(floor);
         }

         /**
          * Returns the time to use as the starting time.
          *
          * @param arguments the function arguments
          * @return the time to use as the starting time
          */
         private long getStartingTime(Arguments arguments)
         {
             if (arguments.size() == 3)
                 return arguments.getAsLong(2);

             return ZERO;
         }

         /**
          * Validates that the duration has the correct precision.
          * @param duration the duration to validate.
          */
         protected void validateDuration(Duration duration)
         {
             if (!duration.hasMillisecondPrecision())
                 throw invalidRequest("The floor cannot be computed for the %s duration as precision is below 1 millisecond", duration);
         }

         /**
          * Serializes the specified time.
          *
          * @param timeInMillis the time in milliseconds
          * @return the serialized time
          */
         protected abstract ByteBuffer fromTimeInMillis(long timeInMillis);
     }

    /**
     * Function that rounds a timestamp down to the closest multiple of a duration.
     */
     public static final class FloorTimestampFunction extends FloorFunction
     {
         public static FloorTimestampFunction newInstance()
         {
             return new FloorTimestampFunction(TimestampType.instance,
                                               TimestampType.instance,
                                               DurationType.instance);
         }

         public static FloorTimestampFunction newInstanceWithStartTimeArgument()
         {
             return new FloorTimestampFunction(TimestampType.instance,
                                               TimestampType.instance,
                                               DurationType.instance,
                                               TimestampType.instance);
         }

         private FloorTimestampFunction(AbstractType<?> returnType,
                                        AbstractType<?>... argTypes)
         {
             super(returnType, argTypes);
         }

         protected ByteBuffer fromTimeInMillis(long timeInMillis)
         {
             return TimestampType.instance.fromTimeInMillis(timeInMillis);
         }
     }

     /**
      * Function that rounds a timeUUID down to the closest multiple of a duration.
      */
     public static final class FloorTimeUuidFunction extends FloorFunction
     {
         public static FloorTimeUuidFunction newInstance()
         {
             return new FloorTimeUuidFunction(TimestampType.instance,
                                              TimeUUIDType.instance,
                                              DurationType.instance);
         }

         public static FloorTimeUuidFunction newInstanceWithStartTimeArgument()
         {
             return new FloorTimeUuidFunction(TimestampType.instance,
                                              TimeUUIDType.instance,
                                              DurationType.instance,
                                              TimestampType.instance);
         }

         private FloorTimeUuidFunction(AbstractType<?> returnType,
                                       AbstractType<?>... argTypes)
         {
             super(returnType, argTypes);
         }

         protected ByteBuffer fromTimeInMillis(long timeInMillis)
         {
             return TimestampType.instance.fromTimeInMillis(timeInMillis);
         }
     }

     /**
      * Function that rounds a date down to the closest multiple of a duration.
      */
     public static final class FloorDateFunction extends FloorFunction
     {
         public static FloorDateFunction newInstance()
         {
             return new FloorDateFunction(SimpleDateType.instance,
                                          SimpleDateType.instance,
                                          DurationType.instance);
         }

         public static FloorDateFunction newInstanceWithStartTimeArgument()
         {
             return new FloorDateFunction(SimpleDateType.instance,
                                          SimpleDateType.instance,
                                          DurationType.instance,
                                          SimpleDateType.instance);
         }

         private FloorDateFunction(AbstractType<?> returnType,
                                   AbstractType<?>... argTypes)
         {
             super(returnType, argTypes);
         }

         protected ByteBuffer fromTimeInMillis(long timeInMillis)
         {
             return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
         }

         @Override
         protected void validateDuration(Duration duration)
         {
             // Checks that the duration has no data below days.
             if (duration.getNanoseconds() != 0)
                 throw invalidRequest("The floor on %s values cannot be computed for the %s duration as precision is below 1 day",
                                      SimpleDateType.instance.asCQL3Type(), duration);
         }
     }

     /**
      * Function that rounds a time down to the closest multiple of a duration.
      */
     public static final NativeScalarFunction floorTime = new NativeScalarFunction("floor", TimeType.instance, TimeType.instance, DurationType.instance)
     {
         @Override
         protected boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters)
         {
             return partialParameters.get(0) == UNRESOLVED && partialParameters.get(1) != UNRESOLVED;
         }

         @Override
         public ByteBuffer execute(Arguments arguments)
         {
             if (arguments.containsNulls())
                 return null;

             long time = arguments.getAsLong(0);
             Duration duration = arguments.get(1);

             long floor = Duration.floorTime(time, duration);

             return TimeType.instance.decompose(floor);
         }
     };
 }

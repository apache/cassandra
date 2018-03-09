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
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public abstract class TimeFcts
{
    public static Logger logger = LoggerFactory.getLogger(TimeFcts.class);

    public static Collection<Function> all()
    {
        return ImmutableList.of(now("now", TimeUUIDType.instance),
                                now("currenttimeuuid", TimeUUIDType.instance),
                                now("currenttimestamp", TimestampType.instance),
                                now("currentdate", SimpleDateType.instance),
                                now("currenttime", TimeType.instance),
                                minTimeuuidFct,
                                maxTimeuuidFct,
                                dateOfFct,
                                unixTimestampOfFct,
                                toDate(TimeUUIDType.instance),
                                toTimestamp(TimeUUIDType.instance),
                                toUnixTimestamp(TimeUUIDType.instance),
                                toUnixTimestamp(TimestampType.instance),
                                toDate(SimpleDateType.instance),
                                toUnixTimestamp(SimpleDateType.instance),
                                toTimestamp(SimpleDateType.instance));
    }

    public static final Function now(final String name, final TemporalType<?> type)
    {
        return new NativeScalarFunction(name, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                return type.now();
            }
        };
    };

    public static final Function minTimeuuidFct = new NativeScalarFunction("mintimeuuid", TimeUUIDType.instance, TimestampType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return UUIDGen.toByteBuffer(UUIDGen.minTimeUUID(TimestampType.instance.compose(bb).getTime()));
        }
    };

    public static final Function maxTimeuuidFct = new NativeScalarFunction("maxtimeuuid", TimeUUIDType.instance, TimestampType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return UUIDGen.toByteBuffer(UUIDGen.maxTimeUUID(TimestampType.instance.compose(bb).getTime()));
        }
    };

    /**
     * Function that convert a value of <code>TIMEUUID</code> into a value of type <code>TIMESTAMP</code>.
     * @deprecated Replaced by the {@link #timeUuidToTimestamp} function
     */
    public static final NativeScalarFunction dateOfFct = new NativeScalarFunction("dateof", TimestampType.instance, TimeUUIDType.instance)
    {
        private volatile boolean hasLoggedDeprecationWarning;

        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            if (!hasLoggedDeprecationWarning)
            {
                hasLoggedDeprecationWarning = true;
                logger.warn("The function 'dateof' is deprecated." +
                            " Use the function 'toTimestamp' instead.");
            }

            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            long timeInMillis = UUIDGen.unixTimestamp(UUIDGen.getUUID(bb));
            return ByteBufferUtil.bytes(timeInMillis);
        }
    };

    /**
     * Function that convert a value of type <code>TIMEUUID</code> into an UNIX timestamp.
     * @deprecated Replaced by the {@link #timeUuidToUnixTimestamp} function
     */
    public static final NativeScalarFunction unixTimestampOfFct = new NativeScalarFunction("unixtimestampof", LongType.instance, TimeUUIDType.instance)
    {
        private volatile boolean hasLoggedDeprecationWarning;

        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            if (!hasLoggedDeprecationWarning)
            {
                hasLoggedDeprecationWarning = true;
                logger.warn("The function 'unixtimestampof' is deprecated." +
                            " Use the function 'toUnixTimestamp' instead.");
            }

            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return ByteBufferUtil.bytes(UUIDGen.unixTimestamp(UUIDGen.getUUID(bb)));
        }
    };

   /**
    * Creates a function that convert a value of the specified type into a <code>DATE</code>.
    * @param type the temporal type
    * @return a function that convert a value of the specified type into a <code>DATE</code>.
    */
   public static final NativeScalarFunction toDate(final TemporalType<?> type)
   {
       return new NativeScalarFunction("todate", SimpleDateType.instance, type)
       {
           public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
           {
               ByteBuffer bb = parameters.get(0);
               if (bb == null || !bb.hasRemaining())
                   return null;

               long millis = type.toTimeInMillis(bb);
               return SimpleDateType.instance.fromTimeInMillis(millis);
           }
       };
   }

   /**
    * Creates a function that convert a value of the specified type into a <code>TIMESTAMP</code>.
    * @param type the temporal type
    * @return a function that convert a value of the specified type into a <code>TIMESTAMP</code>.
    */
   public static final NativeScalarFunction toTimestamp(final TemporalType<?> type)
   {
       return new NativeScalarFunction("totimestamp", TimestampType.instance, type)
       {
           public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
           {
               ByteBuffer bb = parameters.get(0);
               if (bb == null || !bb.hasRemaining())
                   return null;

               long millis = type.toTimeInMillis(bb);
               return TimestampType.instance.fromTimeInMillis(millis);
           }
       };
   }

    /**
     * Creates a function that convert a value of the specified type into an UNIX timestamp.
     * @param type the temporal type
     * @return a function that convert a value of the specified type into an UNIX timestamp.
     */
    public static final NativeScalarFunction toUnixTimestamp(final TemporalType<?> type)
    {
        return new NativeScalarFunction("tounixtimestamp", LongType.instance, type)
        {
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer bb = parameters.get(0);
                if (bb == null || !bb.hasRemaining())
                    return null;

                return ByteBufferUtil.bytes(type.toTimeInMillis(bb));
            }
        };
    }
}


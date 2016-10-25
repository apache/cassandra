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
import java.util.Date;
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
        return ImmutableList.of(nowFct,
                                minTimeuuidFct,
                                maxTimeuuidFct,
                                dateOfFct,
                                unixTimestampOfFct,
                                timeUuidtoDate,
                                timeUuidToTimestamp,
                                timeUuidToUnixTimestamp,
                                timestampToUnixTimestamp,
                                timestampToDate,
                                dateToUnixTimestamp,
                                dateToTimestamp);
    }

    public static final Function nowFct = new NativeScalarFunction("now", TimeUUIDType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            return ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
        }
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
     * Function that convert a value of <code>TIMEUUID</code> into a value of type <code>DATE</code>.
     */
    public static final NativeScalarFunction timeUuidtoDate = new NativeScalarFunction("todate", SimpleDateType.instance, TimeUUIDType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            long timeInMillis = UUIDGen.unixTimestamp(UUIDGen.getUUID(bb));
            return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
        }
    };

    /**
     * Function that convert a value of type <code>TIMEUUID</code> into a value of type <code>TIMESTAMP</code>.
     */
    public static final NativeScalarFunction timeUuidToTimestamp = new NativeScalarFunction("totimestamp", TimestampType.instance, TimeUUIDType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            long timeInMillis = UUIDGen.unixTimestamp(UUIDGen.getUUID(bb));
            return TimestampType.instance.fromTimeInMillis(timeInMillis);
        }
    };

    /**
     * Function that convert a value of type <code>TIMEUUID</code> into an UNIX timestamp.
     */
    public static final NativeScalarFunction timeUuidToUnixTimestamp = new NativeScalarFunction("tounixtimestamp", LongType.instance, TimeUUIDType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return ByteBufferUtil.bytes(UUIDGen.unixTimestamp(UUIDGen.getUUID(bb)));
        }
    };

    /**
     * Function that convert a value of type <code>TIMESTAMP</code> into an UNIX timestamp.
     */
    public static final NativeScalarFunction timestampToUnixTimestamp = new NativeScalarFunction("tounixtimestamp", LongType.instance, TimestampType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            Date date = TimestampType.instance.compose(bb);
            return date == null ? null : ByteBufferUtil.bytes(date.getTime());
        }
    };

   /**
    * Function that convert a value of type <code>TIMESTAMP</code> into a <code>DATE</code>.
    */
   public static final NativeScalarFunction timestampToDate = new NativeScalarFunction("todate", SimpleDateType.instance, TimestampType.instance)
   {
       public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
       {
           ByteBuffer bb = parameters.get(0);
           if (bb == null)
               return null;

           Date date = TimestampType.instance.compose(bb);
           return date == null ? null : SimpleDateType.instance.fromTimeInMillis(date.getTime());
       }
   };

   /**
    * Function that convert a value of type <code>TIMESTAMP</code> into a <code>DATE</code>.
    */
   public static final NativeScalarFunction dateToTimestamp = new NativeScalarFunction("totimestamp", TimestampType.instance, SimpleDateType.instance)
   {
       public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
       {
           ByteBuffer bb = parameters.get(0);
           if (bb == null)
               return null;

           long millis = SimpleDateType.instance.toTimeInMillis(bb);
           return TimestampType.instance.fromTimeInMillis(millis);
       }
   };

   /**
    * Function that convert a value of type <code>DATE</code> into an UNIX timestamp.
    */
   public static final NativeScalarFunction dateToUnixTimestamp = new NativeScalarFunction("tounixtimestamp", LongType.instance, SimpleDateType.instance)
   {
       public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
       {
           ByteBuffer bb = parameters.get(0);
           if (bb == null)
               return null;

           return ByteBufferUtil.bytes(SimpleDateType.instance.toTimeInMillis(bb));
       }
   };
}


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

package org.apache.cassandra.net;

import java.io.EOFException;
import java.nio.BufferOverflowException;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class LogbackFilter extends Filter
{
    private static final Pattern ignore = Pattern.compile("(successfully connected|connection established), version =");

    public FilterReply decide(Object o)
    {
        if (!(o instanceof LoggingEvent))
            return FilterReply.NEUTRAL;

        LoggingEvent e = (LoggingEvent) o;
//        if (ignore.matcher(e.getMessage()).find())
//            return FilterReply.DENY;

        IThrowableProxy t = e.getThrowableProxy();
        if (t == null)
            return FilterReply.NEUTRAL;

        if (!isIntentional(t))
            return FilterReply.NEUTRAL;

//        logger.info("Filtered exception {}: {}", t.getClassName(), t.getMessage());
        return FilterReply.DENY;
    }

    private static final Set<String> intentional = ImmutableSet.of(
        Connection.IntentionalIOException.class.getName(),
        Connection.IntentionalRuntimeException.class.getName(),
        InvalidSerializedSizeException.class.getName(),
        BufferOverflowException.class.getName(),
        EOFException.class.getName()
    );

    public static boolean isIntentional(IThrowableProxy t)
    {
        while (true)
        {
            if (intentional.contains(t.getClassName()))
                return true;

            if (null == t.getCause())
                return false;

            t = t.getCause();
        }
    }


}

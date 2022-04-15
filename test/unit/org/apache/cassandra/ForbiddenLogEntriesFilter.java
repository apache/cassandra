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

package org.apache.cassandra;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import io.netty.util.ResourceLeakDetector;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.logging.LogbackLoggingSupport;
import org.apache.cassandra.utils.logging.LoggingSupportFactory;

public class ForbiddenLogEntriesFilter extends Filter<ILoggingEvent>
{
    private static final Set<String> followedLoggerNames = Sets.newConcurrentHashSet(ImmutableSet.of(
    LoggerFactory.getLogger(ResourceLeakDetector.class).getName(),
    LoggerFactory.getLogger(Ref.class).getName()));

    private volatile Consumer<ILoggingEvent> listener = null;

    private final Set<String> expectedPhrases = new CopyOnWriteArraySet<>();

    public static ForbiddenLogEntriesFilter getInstanceIfUsed()
    {
        if (!(LoggingSupportFactory.getLoggingSupport() instanceof LogbackLoggingSupport))
            return null;

        LogbackLoggingSupport loggingSupport = (LogbackLoggingSupport) LoggingSupportFactory.getLoggingSupport();
        return loggingSupport.getAllLogbackFilters().stream().filter(ForbiddenLogEntriesFilter.class::isInstance)
                             .map(ForbiddenLogEntriesFilter.class::cast).findFirst().orElse(null);
    }

    public void addLoggerToFollow(String name)
    {
        followedLoggerNames.add(name);
    }

    public void addLoggerToFollow(Class<?> clazz)
    {
        followedLoggerNames.add(LoggerFactory.getLogger(clazz).getName());
    }

    public void addLoggerToFollow(Logger logger)
    {
        followedLoggerNames.add(logger.getName());
    }

    public void addExpectedPhrase(String phrase)
    {
        expectedPhrases.add(phrase);
    }

    public void clearExpectedPhrases()
    {
        expectedPhrases.clear();
    }

    public void setListener(Consumer<ILoggingEvent> listener)
    {
        this.listener = listener;
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        Consumer<ILoggingEvent> listener = this.listener;
        if (listener == null)
            return FilterReply.NEUTRAL;

        // we are only interested in error messages
        if (event.getLevel() != Level.ERROR)
            return FilterReply.NEUTRAL;

        // we are only interested in messages from the specified classes
        if (!followedLoggerNames.contains(event.getLoggerName()))
            return FilterReply.NEUTRAL;

        // we skip messages containing one of the specified phrases
        if (expectedPhrases.stream().anyMatch(msg -> event.getFormattedMessage().contains(msg)))
            return FilterReply.NEUTRAL;

        listener.accept(event);

        return FilterReply.NEUTRAL;
    }
}

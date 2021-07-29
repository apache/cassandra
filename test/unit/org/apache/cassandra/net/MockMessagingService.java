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

import java.net.UnknownHostException;
import java.util.function.Predicate;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Starting point for mocking {@link MessagingService} interactions. Outgoing messages can be
 * intercepted by first creating a {@link MatcherResponse} by calling {@link MockMessagingService#when(Matcher)}.
 * Alternatively {@link Matcher}s can be created by using helper methods such as {@link #to(InetAddressAndPort)},
 * {@link #verb(Verb)} or {@link #payload(Predicate)} and may also be
 * nested using {@link MockMessagingService#all(Matcher[])} or {@link MockMessagingService#any(Matcher[])}.
 * After each test, {@link MockMessagingService#cleanup()} must be called for free listeners registered
 * in {@link MessagingService}.
 */
public class MockMessagingService
{

    private MockMessagingService()
    {
    }

    /**
     * Creates a MatcherResponse based on specified matcher.
     */
    public static MatcherResponse when(Matcher matcher)
    {
        return new MatcherResponse(matcher);
    }

    /**
     * Unsubscribes any handlers.
     * This should be called after each test.
     */
    public static void cleanup()
    {
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().inboundSink.clear();
    }

    /**
     * Creates a matcher that will indicate if the target address of the outgoing message equals the
     * provided address.
     */
    public static Matcher<InetAddressAndPort> to(String address)
    {
        try
        {
            return to(InetAddressAndPort.getByName(address));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a matcher that will indicate if the target address of the outgoing message equals the
     * provided address.
     */
    public static Matcher<InetAddressAndPort> to(InetAddressAndPort address)
    {
        return (in, to) -> to == address || to.equals(address);
    }

    /**
     * Creates a matcher that will indicate if the target address of the outgoing message matches the provided predicate.
     */
    public static Matcher<InetAddressAndPort> to(Predicate<InetAddressAndPort> predicate)
    {
        return (in, to) -> predicate.test(to);
    }

    /**
     * Creates a matcher that will indicate if the verb of the outgoing message equals the
     * provided value.
     */
    public static Matcher<Verb> verb(Verb verb)
    {
        return (in, to) -> in.verb() == verb;
    }

    /**
     * Creates a matcher based on the result of the provided predicate called with the outgoing message.
     */
    public static <T> Matcher<T> message(Predicate<Message<T>> fn)
    {
        return (msg, to) -> fn.test(msg);
    }

    /**
     * Creates a matcher based on the result of the provided predicate called with the outgoing message's payload.
     */
    public static <T> Matcher<T> payload(Predicate<T> fn)
    {
        return (msg, to) -> fn.test(msg.payload);
    }

    /**
     * Inverts boolean result of wrapped matcher.
     */
    public static <T> Matcher<T> not(Matcher<T> matcher)
    {
        return (o, to) -> !matcher.matches(o, to);
    }

    /**
     * Indicates true in case all provided matchers returned true.
     */
    public static <T> Matcher<?> all(Matcher<?>... matchers)
    {
        return (Message<T> out, InetAddressAndPort to) -> {
            for (Matcher matcher : matchers)
            {
                if (!matcher.matches(out, to))
                    return false;
            }
            return true;
        };
    }

    /**
     * Indicates true in case at least a single provided matcher returned true.
     */
    public static <T> Matcher<?> any(Matcher<?>... matchers)
    {
        return (Message<T> out, InetAddressAndPort to) -> {
            for (Matcher matcher : matchers)
            {
                if (matcher.matches(out, to))
                    return true;
            }
            return false;
        };
    }
}

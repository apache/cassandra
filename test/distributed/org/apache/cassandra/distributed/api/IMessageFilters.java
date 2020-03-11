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

package org.apache.cassandra.distributed.api;

import java.util.function.Predicate;

public interface IMessageFilters
{
    public interface Filter
    {
        Filter off();
        Filter on();
    }

    public interface Builder
    {
        Builder from(int ... nums);
        Builder to(int ... nums);

        Builder verbs(int... verbs);
        Builder allVerbs();

        Builder inbound(boolean inbound);

        default Builder inbound()
        {
            return inbound(true);
        }

        default Builder outbound()
        {
            return inbound(false);
        }

        /**
         * Every message for which matcher returns `true` will be _dropped_ (assuming all
         * other matchers in the chain will return `true` as well).
         */
        Builder messagesMatching(Matcher filter);
        Filter drop();
    }

    public interface Matcher
    {
        boolean matches(int from, int to, IMessage message);

        static Matcher of(Predicate<IMessage> fn) {
            return (from, to, m) -> fn.test(m);
        }
    }

    Builder inbound(boolean inbound);
    default Builder inbound() {
        return inbound(true);
    }
    default Builder outbound() {
        return inbound(false);
    }
    default Builder verbs(int... verbs) {
        return inbound().verbs(verbs);
    }
    default Builder allVerbs() {
        return inbound().allVerbs();
    }
    void reset();

    /**
     * Checks if the message should be delivered.  This is expected to run on "inbound", or on the reciever of
     * the message (instance.config.num == to).
     *
     * @return {@code true} value returned by the implementation implies that the message was
     * not matched by any filters and therefore should be delivered.
     */
    boolean permitInbound(int from, int to, IMessage msg);

    /**
     * Checks if the message should be delivered.  This is expected to run on "outbound", or on the sender of
     * the message (instance.config.num == from).
     *
     * @return {@code true} value returned by the implementation implies that the message was
     * not matched by any filters and therefore should be delivered.
     */
    boolean permitOutbound(int from, int to, IMessage msg);
}

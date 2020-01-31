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
    }

    Builder verbs(int... verbs);
    Builder allVerbs();
    void reset();

    /**
     * {@code true} value returned by the implementation implies that the message was
     * not matched by any filters and therefore should be delivered.
     */
    boolean permit(int from, int to, IMessage msg);
}

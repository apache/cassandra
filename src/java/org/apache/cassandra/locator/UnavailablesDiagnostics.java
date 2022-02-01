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

package org.apache.cassandra.locator;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.exceptions.UnavailableException;

public class UnavailablesDiagnostics
{

    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private UnavailablesDiagnostics()
    {
    }

    public static void forWriteException(UnavailableException e, ReplicaPlan.ForTokenWrite plan)
    {
        if (!service.isEnabled(UnavailableEvent.class, UnavailableEvent.UnavailableEventType.FOR_WRITE))
            return;
        service.publish(new UnavailableEvent(UnavailableEvent.UnavailableEventType.FOR_WRITE, e, plan, plan.contacts().endpoints()));
    }

    public static void forReadException(UnavailableException e, ReplicaPlan.ForTokenRead plan, Token token)
    {
        if (!service.isEnabled(UnavailableEvent.class, UnavailableEvent.UnavailableEventType.FOR_READ))
            return;
        service.publish(new UnavailableEvent(UnavailableEvent.UnavailableEventType.FOR_READ, e, plan, plan.contacts().endpoints(), token));
    }

    public static void forRangeReadException(UnavailableException e, ReplicaPlan.ForRangeRead plan)
    {
        if (!service.isEnabled(UnavailableEvent.class, UnavailableEvent.UnavailableEventType.FOR_RANGE_READ))
            return;
        service.publish(new UnavailableEvent(UnavailableEvent.UnavailableEventType.FOR_RANGE_READ, e, plan, plan.contacts().endpoints(), plan.range()));
    }

    public static void forPaxosWriteException(UnavailableException e, ReplicaPlan.ForPaxosWrite plan)
    {
        if (!service.isEnabled(UnavailableEvent.class, UnavailableEvent.UnavailableEventType.FOR_PAXOS_WRITE))
            return;
        service.publish(new UnavailableEvent(UnavailableEvent.UnavailableEventType.FOR_PAXOS_WRITE, e, plan, plan.contacts().endpoints()));
    }

    public static void forTruncate(UnavailableException e)
    {
        if (!service.isEnabled(UnavailableEvent.class, UnavailableEvent.UnavailableEventType.FOR_TRUNCATE))
            return;
        service.publish(new UnavailableEvent(UnavailableEvent.UnavailableEventType.FOR_TRUNCATE, e));
    }

    public static void forCounterUpdate(UnavailableException e)
    {
        if (!service.isEnabled(UnavailableEvent.class, UnavailableEvent.UnavailableEventType.FOR_COUNTER))
            return;
        service.publish(new UnavailableEvent(UnavailableEvent.UnavailableEventType.FOR_COUNTER, e));
    }

}

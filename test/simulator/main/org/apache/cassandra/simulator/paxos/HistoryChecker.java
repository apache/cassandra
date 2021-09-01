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

package org.apache.cassandra.simulator.paxos;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Integer.max;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.Witness.Type.READ;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.Witness.Type.SELF;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.Witness.Type.UPDATE_SUCCESS;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.Witness.Type.UPDATE_UNKNOWN;

/**
 * Linearizability checker.
 * <p>
 * Since the partition maintains two total histories of successful operations, we simply verify that this history
 * is consistent with what we view, that each copy of the history is consistent, and that there is no viewing
 * histories backwards or forwards in time (i.e. that the time periods each history is witnessable for are disjoint)
 */
class HistoryChecker
{
    private static final Logger logger = LoggerFactory.getLogger(HistoryChecker.class);

    static class Witness
    {
        enum Type { UPDATE_SUCCESS, UPDATE_UNKNOWN, READ, SELF }

        final Witness.Type type;
        final int eventId;
        final int start;
        final int end;

        Witness(Witness.Type type, int eventId, int start, int end)
        {
            this.type = type;
            this.eventId = eventId;
            this.start = start;
            this.end = end;
        }

        public String toString()
        {
            return String.format("((%3d,%3d),%s,%3d)", start, end, type, eventId);
        }
    }

    static class VerboseWitness extends Witness
    {
        final int[] witnessSequence;

        VerboseWitness(int eventId, int start, int end, int[] witnessSequence)
        {
            super(SELF, eventId, start, end);
            this.witnessSequence = witnessSequence;
        }

        public String toString()
        {
            return String.format("((%3d,%3d), WITNESS, %s)", start, end, Arrays.toString(witnessSequence));
        }
    }

    static class Event
    {
        final List<Witness> log = new ArrayList<>();

        final int eventId;
        int eventPosition = -1;
        int[] witnessSequence;
        int visibleBy = Integer.MAX_VALUE; // witnessed by at least this time
        int visibleUntil = -1;             // witnessed until at least this time (i.e. witnessed nothing newer by then)
        Boolean result;                    // unknown, success or (implied by not being witnessed) failure

        Event(int eventId)
        {
            this.eventId = eventId;
        }
    }

    final int primaryKey;
    private final Queue<Event> unwitnessed = new ArrayDeque<>();
    private Event[] byId = new Event[128];
    private Event[] events = new Event[16];

    HistoryChecker(int primaryKey)
    {
        this.primaryKey = primaryKey;
    }

    Event byId(int id)
    {
        if (byId.length <= id)
            byId = Arrays.copyOf(byId, Math.max(id, byId.length * 2));
        return byId[id];
    }

    Event setById(int id, Event event)
    {
        if (byId.length <= id)
            byId = Arrays.copyOf(byId, Math.max(id, byId.length * 2));
        return byId[id] = event;
    }

    void witness(Observation witness, int[] witnessSequence, int start, int end)
    {
        int eventPosition = witnessSequence.length;
        int eventId = eventPosition == 0 ? -1 : witnessSequence[eventPosition - 1];
        setById(witness.id, new Event(witness.id)).log.add(new VerboseWitness(witness.id, start, end, witnessSequence));
        Event event = get(eventPosition, eventId);
        recordWitness(event, witness, witnessSequence);
        recordVisibleBy(event, end);
        recordVisibleUntil(event, start);

        // see if any of the unwitnessed events can be ruled out
        if (!unwitnessed.isEmpty())
        {
            Iterator<Event> iter = unwitnessed.iterator();
            while (iter.hasNext())
            {
                Event e = iter.next();
                if (e.visibleBy < start)
                {
                    if (e.result == null)
                    {
                        // still accessible byId, so if we witness it later we will flag the inconsistency
                        e.result = FALSE;
                        iter.remove();
                    }
                    else if (e.result)
                    {
                        throw fail(primaryKey, "%d witnessed as absent by %d", e.eventId, witness.id);
                    }
                }
            }
        }
    }

    void applied(int eventId, int start, int end, boolean success)
    {
        Event event = byId(eventId);
        if (event == null)
        {
            setById(eventId, event = new Event(eventId));
            unwitnessed.add(event);
        }

        event.log.add(new Witness(success ? UPDATE_SUCCESS : UPDATE_UNKNOWN, eventId, start, end));
        recordVisibleUntil(event, start);
        recordVisibleBy(event, end); // even the result is unknown, the result must be visible to other operations by the time we terminate
        if (success)
        {
            if (event.result == FALSE)
                throw fail(primaryKey, "witnessed absence of %d but event returned success", eventId);
            event.result = TRUE;
        }
    }

    void recordWitness(Event event, Observation witness, int[] witnessSequence)
    {
        recordWitness(event, witness, witnessSequence.length, witnessSequence);
    }

    void recordWitness(Event event, Observation witness, int eventPosition, int[] witnessSequence)
    {
        while (true)
        {
            event.log.add(new Witness(READ, witness.id, witness.start, witness.end));
            if (event.witnessSequence != null)
            {
                if (!Arrays.equals(event.witnessSequence, witnessSequence))
                    throw fail(primaryKey, "%s previously witnessed %s", witnessSequence, event.witnessSequence);
                return;
            }

            event.witnessSequence = witnessSequence;
            event.eventPosition = eventPosition;

            event = prev(event);
            if (event == null)
                break;

            if (event.witnessSequence != null)
            {
                // verify it's a strict prefix
                if (!equal(event.witnessSequence, witnessSequence, witnessSequence.length - 1))
                    throw fail(primaryKey, "%s previously witnessed %s", witnessSequence, event.witnessSequence);
                break;
            }

            // if our predecessor event hasn't been witnessed directly, witness it by this event, even if
            // we say nothing about the times it may have been witnessed (besides those implied by the write event)
            eventPosition -= 1;
            witnessSequence = Arrays.copyOf(witnessSequence, eventPosition);
        }
    }

    void recordVisibleBy(Event event, int visibleBy)
    {
        if (visibleBy < event.visibleBy)
        {
            event.visibleBy = visibleBy;
            Event prev = prev(event);
            if (prev != null && prev.visibleUntil >= visibleBy)
                throw fail(primaryKey, "%s not witnessed >= %d, but also witnessed <= %d", event.witnessSequence, event.eventId, prev.visibleUntil, event.visibleBy);
        }
    }

    void recordVisibleUntil(Event event, int visibleUntil)
    {
        if (visibleUntil > event.visibleUntil)
        {
            event.visibleUntil = visibleUntil;
            Event next = next(event);
            if (next != null && visibleUntil >= next.visibleBy)
                throw fail(primaryKey, "%s %d not witnessed >= %d, but also witnessed <= %d", next.witnessSequence, next.eventId, event.visibleUntil, next.visibleBy);
        }
    }

    /**
     * Initialise the Event representing both eventPosition and eventId for witnessing
     */
    Event get(int eventPosition, int eventId)
    {
        if (eventPosition >= events.length)
            events = Arrays.copyOf(events, max(eventPosition + 1, events.length * 2));

        Event event = events[eventPosition];
        if (event == null)
        {
            if (eventId < 0)
            {
                events[eventPosition] = event = new Event(eventId);
            }
            else
            {
                event = byId(eventId);
                if (event != null)
                {
                    if (event.eventPosition >= 0)
                        throw fail(primaryKey, "%d occurs at positions %d and %d", eventId, eventPosition, event.eventPosition);
                    events[eventPosition] = event;
                    unwitnessed.remove(event);
                }
                else
                {
                    setById(eventId, events[eventPosition] = event = new Event(eventId));
                }
            }
        }
        else
        {
            if (eventId != event.eventId)
                throw fail(primaryKey, "(eventId, eventPosition): (%d, %d) != (%d, %d)", eventId, eventPosition, event.eventId, event.eventPosition);
            else if (eventPosition != event.eventPosition)
                throw fail(primaryKey, "%d occurs at positions %d and %d", eventId, eventPosition, event.eventPosition);
        }
        return event;
    }

    Event prev(Event event)
    {
        // we can reach here via recordOutcome without knowing our witnessSequence,
        // in which case we won't know our predecessor event, so we cannot do anything useful
        if (event.witnessSequence == null)
            return null;

        int eventPosition = event.eventPosition - 1;
        if (eventPosition < 0)
            return null;

        // initialise the event, if necessary importing information from byId
        return get(eventPosition, eventPosition == 0 ? -1 : event.witnessSequence[eventPosition - 1]);
    }

    Event next(Event event)
    {
        int eventPosition = event.eventPosition + 1;
        if (eventPosition == 0 || eventPosition >= events.length)
            return null;

        // we cannot initialise the event meaningfully, so just return what is already known (if anything)
        return events[eventPosition];
    }

    void print()
    {
        for (Event e : events)
        {
            if (e == null) break;
            logger.error(String.format("%d: (%4d,%4d) %s %s", primaryKey, e.visibleBy, e.visibleUntil, Arrays.toString(e.witnessSequence), e.log));
        }
        for (Event e : byId)
        {
            if (e == null) continue;
            logger.error("{}: {}", e.eventId, e.log);
        }
    }

    static Error fail(int primaryKey, String message, Object ... params)
    {
        for (int i = 0 ; i < params.length ; ++i)
            if (params[i] instanceof int[]) params[i] = Arrays.toString((int[]) params[i]);
        throw new HistoryViolation(primaryKey, "history violation on " + primaryKey + ": " + String.format(message, params));
    }

    static boolean equal(int[] a, int [] b, int count)
    {
        for (int i = 0 ; i < count ; ++i)
            if (a[i] != b[i])
                return false;
        return true;
    }

    static Integer causedBy(Throwable failure)
    {
        if (failure == null || failure.getMessage() == null)
            return null;

        if (!(failure instanceof HistoryViolation))
            return causedBy(failure.getCause());

        return ((HistoryViolation) failure).primaryKey;
    }
}

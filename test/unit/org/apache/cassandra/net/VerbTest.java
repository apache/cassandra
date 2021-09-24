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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class VerbTest
{
    @Test
    public void idsMatch()
    {
        for (Verb v : Verb.getValues())
            assertEquals(v, Verb.fromId(v.id));
    }

    @Test
    public void verbName()
    {
        // MUTATION_RSP is the first
        Verb v = Verb.getValues().get(0);
        assertEquals("MUTATION_RSP", v.toString());
    }

    @Test
    public void invalidVerbIdThrows()
    {
        boolean threw = false;
        try
        {
            Verb.fromId(-1);
        }
        catch (IllegalArgumentException e)
        {
            threw = true;
        }
        assertTrue("Invalid verb identifier was valid", threw);
    }

    @Test
    public void mutationVerbHasMutationHandler()
    {
        Verb mutationReqVerb = Verb.fromId(Verb.MUTATION_REQ.id);
        assertEquals(MutationVerbHandler.instance, mutationReqVerb.handler());
    }

    @Test
    public void addNewVerbWithConflictingId()
    {
        boolean threw = false;
        try
        {
            addFakeVerb(Verb.UNUSED_CUSTOM_VERB.id, "FAKE_REQ");
        }
        catch (IllegalArgumentException ex)
        {
            threw = true;
        }
        assertTrue("Expected IllegalArgumentException when adding existing verb id", threw);
    }

    @Test
    public void addingTwoVerbsHasDistinctIds()
    {
        Verb FAKE_REQ = addFakeVerb(1, "FAKE_REQ");
        assertNotEquals(null, FAKE_REQ);
        assertEquals(FAKE_REQ, Verb.fromId(FAKE_REQ.id));

        Verb FAKE_REQ2 = addFakeVerb(256, "FAKE_REQ2");
        assertEquals(FAKE_REQ2, Verb.fromId(FAKE_REQ2.id));
        assertNotEquals(FAKE_REQ, Verb.fromId(FAKE_REQ2.id));
    }

    private Verb addFakeVerb(int id, String name)
    {
        return addFakeVerb(id, name, () -> MutationVerbHandler.instance);
    }

    private Verb addFakeVerb(int id, String name, Supplier<? extends IVerbHandler<?>> handler)
    {
        return Verb.addCustomVerb(name, id, Verb.Priority.P0, VerbTimeouts.writeTimeout, Stage.MUTATION, () -> NoPayload.serializer, handler, Verb.MUTATION_RSP);
    }

    private static class CountingVerbHandler implements IVerbHandler<Integer>
    {
        public int count;
        @Override
        public void doVerb(Message<Integer> message)
        {
            count++;
        }
    }

    private static class DoubleCaller implements IVerbHandler<Integer>
    {
        IVerbHandler<Integer> handler;
        public DoubleCaller(IVerbHandler<Integer> handler)
        {
           this.handler = handler;
        }

        @Override
        public void doVerb(Message<Integer> message) throws IOException
        {
            handler.doVerb(message);
            handler.doVerb(message);
        }
    }

    @Test
    public void decorateHandler() throws IOException
    {
        CountingVerbHandler handler = new CountingVerbHandler();
        Verb decoratedVerb = addFakeVerb(23, "DECORATED_VERB", () -> handler);

        Verb.decorateHandler(Arrays.asList(decoratedVerb), (oldHandler) -> new DoubleCaller((IVerbHandler<Integer>) oldHandler));
        decoratedVerb.handler().doVerb(null);
        assertEquals(2, handler.count);
    }
}

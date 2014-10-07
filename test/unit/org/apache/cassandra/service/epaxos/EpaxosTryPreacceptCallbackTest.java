package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;

public class EpaxosTryPreacceptCallbackTest extends AbstractEpaxosTest
{
    public MessageIn<TryPreacceptResponse> createResponse(InetAddress from,
                                                          Instance instance,
                                                          TryPreacceptDecision decision,
                                                          boolean vetoed,
                                                          int ballotFailure)
    {
        TryPreacceptResponse response = new TryPreacceptResponse(instance.getToken(),
                                                                 instance.getCfId(),
                                                                 0,
                                                                 instance.getScope(),
                                                                 instance.getId(),
                                                                 decision,
                                                                 vetoed,
                                                                 ballotFailure);
        return MessageIn.create(from, response, Collections.<String, byte[]>emptyMap(), null, 0);
    }

    /**
     * tests that a successful try preaccept
     * round causes an accept phase
     */
    @Test
    public void acceptCase()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true, // test veto is applied
                                                              false,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        DoNothing fcb = new DoNothing();
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, fcb);

        boolean expectedVeto = true;
        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.ACCEPTED, expectedVeto, 0));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.accepts.size());
        MockCallbackService.AcceptCall call = service.accepts.get(0);
        Assert.assertEquals(id, call.id);
        Assert.assertTrue(call.decision.acceptNeeded);
        Assert.assertEquals(attempt.dependencies, call.decision.acceptDeps);
        Assert.assertEquals(expectedVeto, call.decision.vetoed);
        Assert.assertEquals(fcb, call.failureCallback);
        Assert.assertEquals(0, fcb.timesRun);
    }

    /**
     * tests that an unsuccessful try preaccept
     * round moves onto the next attempt, if one exists
     */
    @Test
    public void nextAttemptCase()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true,
                                                              true,
                                                              null);

        TryPreacceptAttempt attempt2 = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                               Sets.newHashSet(service.localEndpoints.get(2)),
                                                               1,
                                                               Sets.newHashSet(service.localEndpoints.get(0), service.localEndpoints.get(1)),
                                                               true,
                                                               true,
                                                               null);

        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        List<TryPreacceptAttempt> nextAttempts = Lists.newArrayList(attempt2);
        DoNothing cb = new DoNothing();
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, nextAttempts, pi, cb);

        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.REJECTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.REJECTED, false, 0));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.tryPreacceptCalls.size());
        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, cb.timesRun);
        MockCallbackService.TryPreacceptCall call = service.tryPreacceptCalls.get(0);
        Assert.assertEquals(id, call.iid);
        Assert.assertEquals(Lists.newArrayList(attempt2), call.attempts);
        Assert.assertEquals(pi, call.participantInfo);
        Assert.assertEquals(cb, call.failureCallback);
    }

    /**
     * tests that an unsuccessful try preaccept  round falls back to a normal
     * prepare preaccept phase, if there are no other attempts to be made
     */
    @Test
    public void regularPreacceptCase()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true,
                                                              false,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        DoNothing fcb = new DoNothing();
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, fcb);

        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.REJECTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, fcb.timesRun);
        Assert.assertEquals(0, service.tryPreacceptCalls.size());
        Assert.assertEquals(1, service.preacceptPrepares.size());
        MockCallbackService.PreacceptPrepareCall call = service.preacceptPrepares.get(0);
        Assert.assertEquals(id, call.id);
        Assert.assertEquals(false, call.noop);
        Assert.assertEquals(fcb, call.failureCallback);
    }

    /**
     * Tests that the failure callback is called, and the try preaccept round
     * completed if a contended response is received
     */
    @Test
    public void testContended()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true,
                                                              false,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        DoNothing fcb = new DoNothing();
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, fcb);

        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.CONTENDED, false, 0));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(1, fcb.timesRun);
    }

    @Test
    public void attemptEpochVeto()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true,
                                                              true,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, null);

        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.accepts.size());
        MockCallbackService.AcceptCall call = service.accepts.get(0);
        Assert.assertTrue(call.decision.vetoed);
    }

    @Test
    public void responseEpochVeto()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true,
                                                              false,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, null);

        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, true, 0));
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.accepts.size());
        MockCallbackService.AcceptCall call = service.accepts.get(0);
        Assert.assertTrue(call.decision.vetoed);
    }

    /**
     * Tests that multiple responses from the same endpoint, and endpoints we didn't send messages to, are ignored
     */
    @Test
    public void ignoreDuplicateAndUninvolvedResponses()
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true,
                                                              true,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, null);

        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        // duplicate response
        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        // response from node not in attempt.toConvince
        callback.epochResponse(createResponse(service.localEndpoints.get(2), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertFalse(callback.isCompleted());
        callback.epochResponse(createResponse(service.localEndpoints.get(1), instance, TryPreacceptDecision.ACCEPTED, false, 0));
        Assert.assertTrue(callback.isCompleted());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackService service = new MockCallbackService(4, 0);
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        final UUID id = instance.getId();
        // require fewer responses than disagreeing replicas
        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.newCopyOnWriteArraySet(service.localEndpoints.subList(0, 2)),
                                                              2,
                                                              Sets.newHashSet(service.localEndpoints.get(2)),
                                                              true, // test veto is applied
                                                              false,
                                                              null);

        List<TryPreacceptAttempt> next = Lists.newArrayList();
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);
        DoNothing fcb = new DoNothing();
        TryPreacceptCallback callback = new TryPreacceptCallback(service, id, attempt, next, pi, fcb);

        int expectedBallot = 5;
        Assert.assertEquals(0, instance.getBallot());
        Assert.assertFalse(callback.isCompleted());
        callback.epochResponse(createResponse(service.localEndpoints.get(0), instance, TryPreacceptDecision.ACCEPTED, false, expectedBallot));
        Assert.assertTrue(callback.isCompleted());

        // check ballot update
        Assert.assertEquals(1, service.ballotUpdates.size());
        MockCallbackService.UpdateBallotCall call = service.ballotUpdates.get(0);
        Assert.assertEquals(instance.getId(), call.id);
        Assert.assertEquals(expectedBallot, call.ballot);

        Assert.assertEquals(0, fcb.timesRun);  // called by ballot update task
        Assert.assertEquals(fcb, call.callback);
    }
}

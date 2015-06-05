package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * The trypreaccept stuff is tested in EpaxosPrepareCallbackTryPreacceptTest
 */
public class EpaxosPrepareCallbackTest extends AbstractEpaxosTest
{
    public PrepareCallback getCallback(EpaxosService service, Instance instance)
    {
        return getCallback(service, instance, 1);
    }

    public PrepareCallback getCallback(EpaxosService service, Instance instance, int attempt)
    {
        return new PrepareCallback(service,
                                   instance.getId(),
                                   instance.getBallot(),
                                   service.getParticipants(instance),
                                   new PrepareGroup(service, UUIDGen.getTimeUUID(), Sets.newHashSet(instance.getId())),
                                   attempt);
    }
    public MessageIn<MessageEnvelope<Instance>> createResponse(InetAddress from, Instance instance)
    {
        return createResponse(from, instance, instance.getToken(), instance.getCfId(), instance.getScope());
    }

    public MessageIn<MessageEnvelope<Instance>> createResponse(InetAddress from, Instance instance, Token token, UUID cfId, Scope scope)
    {
        return MessageIn.create(from, new MessageEnvelope<>(token, cfId, 0, scope, instance),
                                Collections.<String, byte[]>emptyMap(), null, 0);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.updateBallot(1);

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        int expectedBallot = 10;
        Instance responseCopy = instance.copy();
        responseCopy.updateBallot(expectedBallot);

        callback.response(createResponse(service.localEndpoints.get(1), responseCopy));

        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.ballotUpdates.size());
        MockCallbackService.UpdateBallotCall ballotCall = service.ballotUpdates.get(0);

        Assert.assertEquals(instance.getId(), ballotCall.id);
        Assert.assertEquals(expectedBallot, ballotCall.ballot);
        Assert.assertNotNull(ballotCall.callback);
        Assert.assertTrue(ballotCall.callback instanceof PrepareTask);
    }

    @Test
    public void preacceptDecision() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.preaccept(Sets.newHashSet(dep1));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response from the leader to prevent trypreaccept
        callback.response(createResponse(service.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send another preaccepted response
        callback.response(createResponse(service.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an prepare preaccept call was made
        Assert.assertEquals(1, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        // that the prepare decision calls for an preaccept phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.PREACCEPTED, decision.state);
        Assert.assertNull(decision.deps);
        Assert.assertEquals(instance.getBallot(), decision.ballot);
        Assert.assertEquals(Collections.EMPTY_LIST, decision.tryPreacceptAttempts);
        Assert.assertFalse(decision.commitNoop);

        // check that the call to preacceptPrepare was well formed
        MockCallbackService.PreacceptPrepareCall call = service.preacceptPrepares.get(0);
        Assert.assertEquals(instance.getId(), call.id);
        Assert.assertFalse(call.noop);
        Assert.assertNotNull(call.failureCallback);
    }

    @Test
    public void acceptDecision() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.preaccept(Sets.newHashSet(dep1));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response
        callback.response(createResponse(service.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send an accepted response with different deps
        Set<UUID> expectedDeps = Sets.newHashSet(dep1, dep2);
        instance.accept(expectedDeps);
        callback.response(createResponse(service.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an accept call was made
        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(1, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        // that the prepare decision calls for an accept phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.ACCEPTED, decision.state);
        Assert.assertEquals(expectedDeps, decision.deps);
        Assert.assertEquals(instance.getBallot(), decision.ballot);
        Assert.assertNull(decision.tryPreacceptAttempts);
        Assert.assertFalse(decision.commitNoop);

        // and that the accept call is well formed
        MockCallbackService.AcceptCall acceptCall = service.accepts.get(0);
        Assert.assertEquals(instance.getId(), acceptCall.id);
        Assert.assertEquals(expectedDeps, acceptCall.decision.acceptDeps);
        Assert.assertNotNull(acceptCall.failureCallback);
    }

    @Test
    public void commitDecision() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.accept(Sets.newHashSet(dep1));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send an accepted response
        callback.response(createResponse(service.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send a committed response with different deps
        Set<UUID> expectedDeps = Sets.newHashSet(dep1, dep2);
        instance.commit(expectedDeps);
        callback.response(createResponse(service.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an accept call was made
        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(1, service.commits.size());

        // that the prepare decision calls for a commit phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.COMMITTED, decision.state);
        Assert.assertEquals(expectedDeps, decision.deps);
        Assert.assertEquals(instance.getBallot(), decision.ballot);
        Assert.assertNull(decision.tryPreacceptAttempts);
        Assert.assertFalse(decision.commitNoop);

        // and that the commit call is well formed
        MockCallbackService.CommitCall commitCall = service.commits.get(0);
        Assert.assertEquals(instance.getId(), commitCall.id);
        Assert.assertEquals(expectedDeps, commitCall.dependencies);
    }

    @Test
    public void preacceptNoopDecision() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response from the leader to prevent trypreaccept
        callback.response(createResponse(service.localEndpoints.get(1), null, instance.getToken(), instance.getCfId(), instance.getScope()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send a null responses for the second reply
        callback.response(createResponse(service.localEndpoints.get(2), null, instance.getToken(), instance.getCfId(), instance.getScope()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an prepare preaccept call was made
        Assert.assertEquals(1, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());

        // that the prepare decision calls for an preaccept phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.PREACCEPTED, decision.state);
        Assert.assertNull(decision.deps);
        Assert.assertEquals(Collections.EMPTY_LIST, decision.tryPreacceptAttempts);
        Assert.assertTrue(decision.commitNoop);

        // check that the call to preacceptPrepare was well formed
        MockCallbackService.PreacceptPrepareCall call = service.preacceptPrepares.get(0);
        Assert.assertEquals(instance.getId(), call.id);
        Assert.assertTrue(call.noop);
        Assert.assertNotNull(call.failureCallback);
    }

    /**
     * Check that messages received more than once aren't counted more than once
     */
    @Test
    public void duplicateMessagesIgnored() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response
        callback.response(createResponse(service.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send a duplicate response
        callback.response(createResponse(service.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());
    }

    /**
     * Check that messages coming in after a quorum is reached are ignored
     */
    @Test
    public void additionalMessagesAreIgnored() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send first messages, should change state
        callback.response(createResponse(service.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send second messages, should change state
        callback.response(createResponse(service.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // send third messages, should be ignored
        callback.response(createResponse(service.localEndpoints.get(2), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());
    }

    /**
     * Tests that the callback correctly handles responses with
     * instances it didn't have previously
     */
    @Test
    public void addPreviouslyUnknownInstance() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareGroup group = new PrepareGroup(service, UUIDGen.getTimeUUID(), Sets.newHashSet(instance.getId())) {
            @Override
            protected void submitExecuteTask()
            {
                // no-op
            }
        };
        PrepareCallback callback = new PrepareCallback(service, instance.getId(), 0, service.getParticipants(instance), group, 1);

        Assert.assertTrue(callback.isInstanceUnknown());
        Assert.assertFalse(callback.isCompleted());
        Assert.assertNull(service.loadInstance(instance.getId()));
        Assert.assertTrue(group.prepareIsOutstandingFor(instance.getId()));

        callback.response(createResponse(service.localEndpoints.get(2), instance));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertNotNull(service.loadInstance(instance.getId()));
        Assert.assertFalse(group.prepareIsOutstandingFor(instance.getId()));
    }

    @Test
    public void retryLimitReached() throws InvalidInstanceStateChange
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.updateBallot(1);

        PrepareCallback callback = getCallback(service, instance, PrepareCallback.RETRY_LIMIT);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        int expectedBallot = 10;
        Instance responseCopy = instance.copy();
        responseCopy.updateBallot(expectedBallot);

        callback.response(createResponse(service.localEndpoints.get(1), responseCopy));

        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(0, service.ballotUpdates.size());
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());
    }

    @Test
    public void ballotFailuresArentThrownForCommittedInstances() throws InvalidInstanceStateChange
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.commit(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.updateBallot(1);

        PrepareCallback callback = getCallback(service, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        int expectedBallot = 10;
        Instance responseCopy = instance.copy();
        responseCopy.updateBallot(expectedBallot);

        callback.response(createResponse(service.localEndpoints.get(1), responseCopy));

        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(0, service.ballotUpdates.size());

        Assert.assertEquals(1, service.commits.size());
    }
}

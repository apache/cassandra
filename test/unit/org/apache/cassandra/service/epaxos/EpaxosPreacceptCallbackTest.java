package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;

public class EpaxosPreacceptCallbackTest extends AbstractEpaxosTest
{
    public PreacceptCallback getCallback(EpaxosService service, Instance instance, Runnable failureCallback, boolean forceAccept)
    {
        return new PreacceptCallback(service, instance, service.getParticipants(instance), failureCallback, forceAccept);
    }

    public MessageIn<PreacceptResponse> createResponse(InetAddress from, PreacceptResponse response)
    {
        return MessageIn.create(from, response, Collections.<String, byte[]>emptyMap(), null, 0);
    }

    /**
     * Tests that, in larger replica counts, the callback will wait on a fast path of
     * quorum responses before continuing, if enough replicas are live
     */
    @Test
    public void fastQuorum() throws Exception
    {
        MockCallbackService service = new MockCallbackService(7, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.setDependencies(Collections.<UUID>emptySet());

        EpaxosService.ParticipantInfo participants = service.getParticipants(instance);
        Assert.assertEquals(4, participants.quorumSize);
        Assert.assertEquals(5, participants.fastQuorumSize);
        Assert.assertTrue(participants.quorumExists());
        Assert.assertTrue(participants.fastQuorumExists());

        PreacceptCallback callback = new PreacceptCallback(service, instance, service.getParticipants(instance), null, false);

        List<InetAddress> quorum = participants.liveEndpoints.subList(1, participants.quorumSize);
        List<InetAddress> fastQuorum = participants.liveEndpoints.subList(participants.quorumSize, participants.fastQuorumSize);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        // quorum responds
        for (InetAddress endpoint: quorum)
        {
            callback.response(createResponse(endpoint, PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        }

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(participants.quorumSize, callback.getNumResponses());

        // fast quorum responses
        for (InetAddress endpoint: fastQuorum)
        {
            callback.response(createResponse(endpoint, PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        }

        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(participants.fastQuorumSize, callback.getNumResponses());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertFalse(decision.acceptNeeded);
    }

    @Test
    public void quorumOnly() throws Exception
    {
        MockCallbackService service = new MockCallbackService(7, 0) {
            protected Predicate<InetAddress> livePredicate()
            {
                return new Predicate<InetAddress>()
                {
                    public boolean apply(InetAddress address)
                    {
                        return localEndpoints.subList(0, 4).contains(address);
                    }
                };
            }
        };
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.setDependencies(Collections.<UUID>emptySet());

        EpaxosService.ParticipantInfo participants = service.getParticipants(instance);
        Assert.assertEquals(4, participants.quorumSize);
        Assert.assertEquals(5, participants.fastQuorumSize);
        Assert.assertTrue(participants.quorumExists());
        Assert.assertFalse(participants.fastQuorumExists());

        PreacceptCallback callback = new PreacceptCallback(service, instance, service.getParticipants(instance), null, false);

        List<InetAddress> quorum = participants.liveEndpoints.subList(1, participants.quorumSize);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        // quorum responds
        for (InetAddress endpoint: quorum)
        {
            callback.response(createResponse(endpoint, PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        }

        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(participants.quorumSize, callback.getNumResponses());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertTrue(decision.acceptNeeded);
    }

    @Test
    public void fastPathSuccessCase() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(service, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(1, service.commits.size());
        Assert.assertTrue(callback.isCompleted());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertFalse(decision.acceptNeeded);
        Assert.assertEquals(expectedDeps, decision.acceptDeps);
        Assert.assertEquals(Collections.EMPTY_MAP, decision.missingInstances);
    }

    /**
     * Tests that the accept path is chosen if conflicting responses are received
     */
    @Test
    public void slowPathSuccessCase() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        Set<UUID> expectedDeps = Sets.newHashSet(dep1, dep2);
        instance.setDependencies(Sets.newHashSet(dep1));

        PreacceptCallback callback = getCallback(service, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        // respond with a failure, missing the expected dep, and replying with another
        Instance responseInstance = instance.copy();
        responseInstance.setDependencies(Sets.newHashSet(dep2));
        PreacceptResponse response = PreacceptResponse.failure(instance.getToken(), 0, responseInstance);
        // add a missing instance to the response
        response.missingInstances = Lists.newArrayList((Instance) new QueryInstance(dep2,
                                                                                    getSerializedCQLRequest(0, 1),
                                                                                    InetAddress.getByName("127.0.0.100")));
        callback.response(createResponse(service.localEndpoints.get(1), response));
        Assert.assertEquals(1, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());
        Assert.assertTrue(callback.isCompleted());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertTrue(decision.acceptNeeded);
        Assert.assertEquals(expectedDeps, decision.acceptDeps);
        // check that the missing instance was submitted
        Assert.assertEquals(1, service.missingInstancesAdded.size());
        Assert.assertEquals(dep2, service.missingInstancesAdded.get(0).iterator().next().getId());
    }

    @Test
    public void forcedAcceptSuccessCase() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(service, instance, null, true);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        // the instance should be accepted, even if the
        // accept  decision says it't not neccesary
        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(1, service.accepts.size());
        Assert.assertTrue(callback.isCompleted());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertFalse(decision.acceptNeeded);
        Assert.assertEquals(expectedDeps, decision.acceptDeps);
        Assert.assertEquals(Collections.EMPTY_MAP, decision.missingInstances);
    }

    /**
     * Should complete until the local node has registered a response
     */
    @Test
    public void noLocalResponse() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(service, instance, null, false);

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(service, instance, null, false);

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.ballotFailure(instance.getToken(), instance.getCfId(), 0,
                                                                         DEFAULT_SCOPE, 5)));
        Assert.assertEquals(0, service.accepts.size());
        Assert.assertEquals(0, service.commits.size());
        Assert.assertEquals(1, service.ballotUpdates.size());
        Assert.assertTrue(callback.isCompleted());
    }

    @Test
    public void duplicateMessagesIgnored() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(service, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());

        // should be ignored
        callback.response(createResponse(service.localEndpoints.get(2),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());
    }

    @Test
    public void additionalMessagesIgnored() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(service, instance, null, false);

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.success(instance.getToken(), 0, instance.copy())));
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());
    }

    /**
     * Tests that the correct missing instances are sent
     * to the correct nodes in the case of an accept phase
     */
    @Test
    public void sendMissingInstance() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.setDependencies(Sets.newHashSet(dep1));

        PreacceptCallback callback = getCallback(service, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertFalse(callback.isCompleted());

        // respond with a failure, missing the expected dep, and replying with another
        Instance responseInstance = instance.copy();
        responseInstance.setDependencies(Sets.newHashSet(dep2));
        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.failure(instance.getToken(), 0, responseInstance)));
        Assert.assertTrue(callback.isCompleted());

        // check that the missing instances not retured by the remote
        // node are marked to be included in the accept request
        AcceptDecision decision = callback.getAcceptDecision();
        Map<InetAddress, Set<UUID>> missingInstances = Maps.newHashMap();
        missingInstances.put(service.localEndpoints.get(1), Sets.newHashSet(dep1));
        Assert.assertEquals(missingInstances, decision.missingInstances);
    }

    /**
     * Even if the dependencies from the preaccespt responses merge to be the same
     * as the leader, if any individual response is different, an accept should be run
     */
    @Test
    public void convergingDisagreeingDeps() throws Exception
    {
        MockCallbackService service = new MockCallbackService(5, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        UUID dep3 = UUIDGen.getTimeUUID();
        instance.setDependencies(Sets.newHashSet(dep1, dep2, dep3));

        PreacceptCallback callback = getCallback(service, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertFalse(callback.isCompleted());

        // failure 1
        Instance responseInstance;
        responseInstance = instance.copy();
        responseInstance.setDependencies(Sets.newHashSet(dep1, dep2));
        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.failure(instance.getToken(), 0, responseInstance)));
        Assert.assertFalse(callback.isCompleted());

        // failure 2
        responseInstance = instance.copy();
        responseInstance.setDependencies(Sets.newHashSet(dep2, dep3));
        callback.response(createResponse(service.localEndpoints.get(2),
                                         PreacceptResponse.failure(instance.getToken(), 0, responseInstance)));
        Assert.assertTrue(callback.isCompleted());
        // check that an accept is required, even though the responses resolve to match the leader
        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertTrue(decision.acceptNeeded);
        Assert.assertEquals(Sets.newHashSet(dep1, dep2, dep3), decision.acceptDeps);
    }

    /**
     * Even if the dependencies from the preaccespt responses merge to be the same
     * as the leader, if any individual response is different, an accept should be run
     */
    @Test
    public void convergingDisagreeingRanges() throws Exception
    {
        MockCallbackService service = new MockCallbackService(5, 0);
        TokenInstance instance = new TokenInstance(LOCALHOST, CFID, token(50), range(0, 100), DEFAULT_SCOPE);
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        UUID dep3 = UUIDGen.getTimeUUID();
        instance.setDependencies(Sets.newHashSet(dep1, dep2, dep3));

        PreacceptCallback callback = getCallback(service, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertFalse(callback.isCompleted());

        // failure 1
        TokenInstance responseInstance;
        responseInstance = (TokenInstance) instance.copy();
        responseInstance.setSplitRange(range(0, 75));
        callback.response(createResponse(service.localEndpoints.get(1),
                                         PreacceptResponse.failure(instance.getToken(), 0, responseInstance)));
        Assert.assertFalse(callback.isCompleted());

        // failure 2
        responseInstance = (TokenInstance) instance.copy();
        responseInstance.setSplitRange(range(25, 100));
        callback.response(createResponse(service.localEndpoints.get(2),
                                         PreacceptResponse.failure(instance.getToken(), 0, responseInstance)));
        Assert.assertTrue(callback.isCompleted());
        // check that an accept is required, even though the responses resolve to match the leader
        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertTrue(decision.acceptNeeded);
        Assert.assertEquals(range(0, 100), decision.splitRange);
    }
}

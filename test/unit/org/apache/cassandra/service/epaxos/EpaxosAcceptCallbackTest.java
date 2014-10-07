package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class EpaxosAcceptCallbackTest extends AbstractEpaxosTest
{

    public AcceptCallback getCallback(EpaxosService service, Instance instance, Runnable failureCallback)
    {
        return new AcceptCallback(service, instance, service.getParticipants(instance), failureCallback);
    }

    public MessageIn<AcceptResponse> createResponse(InetAddress from, AcceptResponse response)
    {
        return MessageIn.create(from, response, Collections.<String, byte[]>emptyMap(), null, 0);
    }

    @Test
    public void testSuccessCase() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(service, instance, null);

        callback.response(createResponse(service.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(1, service.commits.size());
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());

        MockCallbackService.CommitCall commitCall = service.commits.get(0);
        Assert.assertEquals(instance.getId(), commitCall.id);
        Assert.assertEquals(instance.getDependencies(), commitCall.dependencies);
    }

    @Test
    public void noLocalResponse() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(service, instance, null);

        callback.response(createResponse(service.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(2), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {

            }
        };

        AcceptCallback callback = getCallback(service, instance, runnable);

        Assert.assertEquals(0, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, service.ballotUpdates.size());

        callback.response(createResponse(service.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, false, 20)));
        Assert.assertEquals(0, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(1, service.ballotUpdates.size());

        MockCallbackService.UpdateBallotCall ballotCall = service.ballotUpdates.get(0);
        Assert.assertEquals(instance.getId(), ballotCall.id);
        Assert.assertEquals(20, ballotCall.ballot);
        Assert.assertEquals(runnable, ballotCall.callback);

    }

    /**
     * Check that messages received more than once aren't counted more than once
     */
    @Test
    public void duplicateMessagesIgnored() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(service, instance, null);

        callback.response(createResponse(service.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertEquals(0, service.commits.size());

        callback.response(createResponse(service.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertEquals(0, service.commits.size());
    }

    /**
     * Check that messages coming in after a quorum is reached are ignored
     */
    @Test
    public void additionalMessagesAreIgnored() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(service, instance, null);

        callback.response(createResponse(service.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, service.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(1, service.commits.size());
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(2), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(1, service.commits.size());
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());
    }

    /**
     * Check that messages from remote endpoints are ignored
     */
    @Test
    public void remoteEndpointsArentCounted() throws Exception
    {
        MockCallbackService service = new MockCallbackService(3, 3);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(service, instance, null);

        callback.response(createResponse(service.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, service.commits.size());
        Assert.assertEquals(1, callback.getNumResponses());

        callback.response(createResponse(service.remoteEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, service.commits.size());
        Assert.assertEquals(1, callback.getNumResponses());
    }

}

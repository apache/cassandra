package org.apache.cassandra.service.epaxos;

import java.util.*;

/**
 * Mocked state for testing the behavior of callback classes
 */
public class MockCallbackService extends MockMessengerService
{
    public MockCallbackService(int numLocal, int numRemote)
    {
        super(numLocal, numRemote);
    }

    public static class PreacceptPrepareCall
    {
        public final UUID id;
        public final boolean noop;
        public final Runnable failureCallback;

        public PreacceptPrepareCall(UUID id, boolean noop, Runnable failureCallback)
        {
            this.id = id;
            this.noop = noop;
            this.failureCallback = failureCallback;
        }
    }

    public final List<PreacceptPrepareCall> preacceptPrepares = new LinkedList<>();

    @Override
    public void preacceptPrepare(UUID id, boolean noop, Runnable failureCallback)
    {
        preacceptPrepares.add(new PreacceptPrepareCall(id, noop, failureCallback));
    }

    public final List<UUID> preaccepts = new LinkedList<>();

    @Override
    public void preaccept(Instance instance)
    {
        preaccepts.add(instance.getId());
    }

    public static class AcceptCall
    {
        public final UUID id;
        public final AcceptDecision decision;
        public final Runnable failureCallback;

        public AcceptCall(UUID id, AcceptDecision decision, Runnable failureCallback)
        {
            this.id = id;
            this.decision = decision;
            this.failureCallback = failureCallback;
        }
    }

    public final List<AcceptCall> accepts = new LinkedList<>();

    @Override
    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        accepts.add(new AcceptCall(iid, decision, failureCallback));
    }

    public static class CommitCall
    {
        public final UUID id;
        public final Set<UUID> dependencies;

        public CommitCall(UUID id, Set<UUID> dependencies)
        {
            this.id = id;
            this.dependencies = dependencies;
        }
    }

    public final List<CommitCall> commits = new LinkedList<>();

    @Override
    public void commit(UUID iid, Set<UUID> dependencies)
    {
        commits.add(new CommitCall(iid, dependencies));
    }

    public final List<UUID> executes = new LinkedList<>();

    @Override
    public void execute(UUID instanceId)
    {
        executes.add(instanceId);
    }

    public final List<UUID> prepares = new LinkedList<>();

    @Override
    public PrepareTask prepare(UUID id, PrepareGroup group)
    {
        prepares.add(id);
        return null;
    }

    public static class UpdateBallotCall
    {
        public final UUID id;
        public final int ballot;
        public final Runnable callback;

        public UpdateBallotCall(UUID id, int ballot, Runnable callback)
        {
            this.id = id;
            this.ballot = ballot;
            this.callback = callback;
        }
    }

    public final List<UpdateBallotCall> ballotUpdates = new LinkedList<>();

    @Override
    public void updateBallot(UUID id, int ballot, Runnable callback)
    {
        ballotUpdates.add(new UpdateBallotCall(id, ballot, callback));
    }

    public static class TryPreacceptCall
    {
        public final UUID iid;
        public final List<TryPreacceptAttempt> attempts;
        public final ParticipantInfo participantInfo;
        public final Runnable failureCallback;

        public TryPreacceptCall(UUID iid, List<TryPreacceptAttempt> attempts, ParticipantInfo participantInfo, Runnable failureCallback)
        {
            this.iid = iid;
            this.attempts = attempts;
            this.participantInfo = participantInfo;
            this.failureCallback = failureCallback;
        }
    }

    public final List<TryPreacceptCall> tryPreacceptCalls = new LinkedList<>();

    @Override
    public void tryPreaccept(UUID iid, List<TryPreacceptAttempt> attempts, ParticipantInfo participantInfo, Runnable failureCallback)
    {
        tryPreacceptCalls.add(new TryPreacceptCall(iid, attempts, participantInfo, failureCallback));
    }

    public final List<Collection<Instance>> missingInstancesAdded = new LinkedList<>();

    @Override
    public void addMissingInstances(Collection<Instance> instances)
    {
        missingInstancesAdded.add(instances);
    }
}

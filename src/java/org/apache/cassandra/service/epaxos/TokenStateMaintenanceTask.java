package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Periodically examines the token states, persisting execution
 * metrics or incrementing epochs when appropriate.
 */
public class TokenStateMaintenanceTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosService.class);

    private final EpaxosService service;
    private final List<TokenStateManager> tsms;

    public TokenStateMaintenanceTask(EpaxosService service, Collection<TokenStateManager> tsms)
    {
        this.service = service;
        assert !tsms.isEmpty();
        this.tsms = new ArrayList<>(tsms);
    }

    protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
    {
        Pair<String, String> cfName = Schema.instance.getCF(cfId);
        if (cfName == null)
            return false;

        Keyspace keyspace = Keyspace.open(cfName.left);
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        Set<InetAddress> replicas = Sets.newHashSet(replicationStrategy.getNaturalEndpoints(token));

        return replicas.contains(FBUtilities.getLocalAddress());
    }

    protected void updateEpochs()
    {
        for (TokenStateManager tsm: tsms)
        {
            Scope scope = tsm.getScope();
            for (UUID cfId: tsm.getAllManagedCfIds())
            {
                if (!service.tableExists(cfId))
                    continue;

                for (Token token: tsm.allTokenStatesForCf(cfId))
                {
                    if (!replicatesTokenForKeyspace(token, cfId))
                        continue;

                    TokenState ts = tsm.getExact(token, cfId);

                    if (!TokenState.State.isUpgraded(ts.getState()))
                        continue;

                    ts.lock.readLock().lock();
                    long currentEpoch;
                    try
                    {
                        currentEpoch = ts.getEpoch();
                        // if this token state is expecting recovery to resume, schedule
                        // a recovery and continue
                        if (ts.getState() == TokenState.State.RECOVERY_REQUIRED)
                        {
                            service.startLocalFailureRecovery(ts.getToken(), ts.getCfId(), 0, tsm.getScope());
                            continue;
                        }
                    }
                    finally
                    {
                        ts.lock.readLock().unlock();
                    }

                    if (ts.getExecutions() >= service.getEpochIncrementThreshold(cfId, scope))
                    {
                        if (ts.getExecutions() < service.getEpochIncrementThreshold(cfId, scope))
                        {
                            continue;
                        }

                        logger.debug("Incrementing epoch for {}", ts);

                        EpochInstance instance = service.createEpochInstance(ts.getToken(), ts.getCfId(), currentEpoch + 1, scope);
                        service.preaccept(instance);
                    }
                    else if (ts.getNumUnrecordedExecutions() > 0)
                    {
                        ts.lock.writeLock().lock();
                        try
                        {
                            logger.debug("Persisting execution data for {}", ts);
                            tsm.save(ts);
                        }
                        finally
                        {
                            ts.lock.writeLock().unlock();
                        }
                    }
                    else
                    {
                        logger.debug("No activity to update for {}", ts);
                    }
                }
            }
        }
    }

    protected Set<Token> getReplicatedTokens(String ksName)
    {
        Set<Range<Token>> ranges = tsms.get(0).getReplicatedRangesForKeyspace(ksName);
        Set<Token> tokens = new HashSet<>();
        for (Range<Token> range: ranges)
        {
            tokens.add(range.right);
        }
        return tokens;
    }

    protected String getKsName(UUID cfId)
    {
        return Schema.instance.getCF(cfId).left;
    }

    /**
     * check that we have token states for all of the tokens we replicate
     */
    protected void checkTokenCoverage()
    {
        for (TokenStateManager tsm: tsms)
        {
            for (UUID cfId: tsm.getAllManagedCfIds())
            {
                if (!service.tableExists(cfId))
                    continue;

                String ksName = getKsName(cfId);
                Set<Token> replicatedTokens = getReplicatedTokens(ksName);

                List<Token> tokens = Lists.newArrayList(replicatedTokens);
                Collections.sort(tokens);

                for (Token token: tokens)
                {
                    if (tsm.getExact(token, cfId) == null && isUpgraded(token, cfId, tsm.getScope()))
                    {
                        logger.info("Running instance for missing token state for token {} on {}", token, cfId);
                        TokenInstance instance = service.createTokenInstance(token, cfId, tsm.getScope());
                        try
                        {
                            service.process(instance);
                        }
                        catch (WriteTimeoutException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    protected boolean isUpgraded(Token token, UUID cfId, Scope scope)
    {
        return UpgradeService.instance().isUpgradedForQuery(token, cfId, scope);
    }

    protected boolean shouldRun()
    {
        return StorageService.instance.inNormalMode();
    }

    @Override
    public void run()
    {
        logger.debug("TokenStateMaintenanceTask running");
        if (!shouldRun())
        {
            logger.debug("Skipping TokenStateMaintenanceTask, node is not in normal mode");
            return;
        }
        checkTokenCoverage();
        updateEpochs();
    }
}

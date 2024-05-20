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

package org.apache.cassandra.tcm;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.transformations.cms.FinishAddToCMS;
import org.apache.cassandra.tools.TransformClusterMetadataHelper;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.tcm.membership.MembershipUtils.nodeAddresses;
import static org.apache.cassandra.tcm.membership.MembershipUtils.randomEndpoint;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.randomPlacements;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.randomTokens;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.bootstrapAndJoin;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.bootstrapAndReplace;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.epoch;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.lockedRanges;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.move;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.unbootstrapAndLeave;
import static org.junit.Assert.assertEquals;

public class BootWithMetadataTest
{
    private static final Logger logger = LoggerFactory.getLogger(BootWithMetadataTest.class);
    /**
     * Starts a test instance, then generates a randomised ClusterMetadata instance, writes it to
     * file and finally re-initialises test instance's ClusterMetadataService. This exercises the
     * same code paths as the counterpart test in o.a.c.distributed.test.log, but in a unit test
     * it is much easier to generate random metadata, which need not actually be valid for a
     * running service, so that we can better exercise the deserialisation code.
     */
    @Test
    public void bootFromExportedMetadataTest() throws Throwable
    {
        // sorting to preserve primary replicas requires real data in Directory
        // and DataPlacements, this test uses completely random data so disable it
        CassandraRelevantProperties.TCM_SORT_REPLICA_GROUPS.setBoolean(false);
        // We will be re-intialising the ClusterMetadata and CMS, which requires
        // us to disable MBean registration. This does not happen outside of tests
        CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        // General test setup, no need to use Paxos for log commits or to replicate
        // to (non-existent) peers
        CassandraRelevantProperties.TCM_USE_ATOMIC_LONG_PROCESSOR.setBoolean(true);
        CassandraRelevantProperties.TCM_USE_NO_OP_REPLICATOR.setBoolean(true);

        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        Epoch epoch = epoch(new Random(System.nanoTime()));
        ClusterMetadata first = ClusterMetadata.current();

        for (int i = 0; i < 100; i++)
            epoch = doTest(Epoch.create(epoch.getEpoch() + 100), first);
    }

    private Epoch doTest(Epoch epoch, ClusterMetadata first) throws Throwable
    {
        long seed = System.nanoTime();
        logger.info("STARTING TEST FROM EPOCH {}, SEED: {}", epoch, seed);
        Random random = new Random(seed);
        ClusterMetadata.Transformer t = first.transformer();
        // Generate a randomised CM in order to exercise the serde code
        // TODO random schema
        IPartitioner partitioner = first.partitioner;
        Directory directory = first.directory;
        int nodeCount = 10;
        int tokensPerNode = 5;
        for (int i = 0; i < nodeCount; i++)
            directory = directory.with(nodeAddresses(random), new Location("DC1", "RACK1"));
        t = t.with(directory);

        Iterator<Token> tokens = randomTokens(nodeCount * tokensPerNode, partitioner, random).iterator();
        Set<Token> perNodeTokens = new HashSet<>(tokensPerNode);
        for (NodeId nodeId : directory.peerIds())
        {
            perNodeTokens.clear();
            for (int i = 0; i < tokensPerNode; i++)
                perNodeTokens.add(tokens.next());
            t = t.proposeToken(nodeId, perNodeTokens);
        };

        DataPlacements placements = randomPlacements(random);
        t = t.with(placements);
        t = t.with(lockedRanges(placements, random));

        InProgressSequences seq = first.inProgressSequences;
        seq = addSequence(seq, bootstrapAndJoin(partitioner, random, seq::contains));
        seq = addSequence(seq, bootstrapAndReplace(partitioner, random, seq::contains));
        seq = addSequence(seq, unbootstrapAndLeave(partitioner, random, seq::contains));
        seq = addSequence(seq, move(partitioner, random, seq::contains));
        seq = addSequence(seq, addToCMS(random, seq::contains));
        t = t.with(seq);
        ClusterMetadata toWrite = t.build().metadata.forceEpoch(epoch);

        // before exporting to file, make the local node the single CMS member in the CM instance
        // as CMS membership is a requirement for re-initialising from file
        toWrite = TransformClusterMetadataHelper.makeCMS(toWrite, FBUtilities.getBroadcastAddressAndPort());

        // export the hand crafted CM to file
        Path path = Files.createTempFile("clustermetadata", "dump");
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(path))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, toWrite, out, NodeVersion.CURRENT_METADATA_VERSION);
        }
        String fileName = path.toString();

        // now re-initialise the local CMS from the file
        Startup.reinitializeWithClusterMetadata(fileName, p -> p, () -> {});

        ClusterMetadata fromRead = ClusterMetadata.current();
        assertEquals(toWrite.schema, fromRead.schema);
        assertEquals(toWrite.directory, fromRead.directory);
        assertEquals(toWrite.tokenMap, fromRead.tokenMap);
        assertEquals(toWrite.placements, fromRead.placements);
        assertEquals(toWrite.lockedRanges, fromRead.lockedRanges);
        assertEquals(toWrite.inProgressSequences, fromRead.inProgressSequences);
        assertEquals(toWrite.extensions, fromRead.extensions);

        return fromRead.epoch;
    }

    private InProgressSequences addSequence(InProgressSequences sequences, MultiStepOperation<?> seq)
    {
        return sequences.with(seq.sequenceKey(), seq);
    }

    private AddToCMS addToCMS(Random random, Predicate<NodeId> alreadyInUse)
    {
        NodeId node = MembershipUtils.node(random);
        while (alreadyInUse.test(node))
            node = MembershipUtils.node(random);

        return new AddToCMS(epoch(random),
                            node,
                            Collections.singleton(randomEndpoint(random)),
                            new FinishAddToCMS(randomEndpoint(random)));
    }
}

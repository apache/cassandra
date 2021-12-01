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

package org.apache.cassandra.nodes;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NodesTest
{
    private final ExecutorService executor = mock(ExecutorService.class);
    private final INodesPersistence persistence = mock(INodesPersistence.class);
    private final Future<?> promise = mock(Future.class);

    private final UUID newHostId = UUID.randomUUID();
    private final UUID id1 = UUID.randomUUID();
    private final UUID id2 = UUID.randomUUID();
    private final UUID id3 = UUID.randomUUID();

    private Nodes nodes;
    private static InetAddressAndPort addr1;
    private static InetAddressAndPort addr2;
    private static InetAddressAndPort addr3;
    private ArgumentCaptor<Runnable> taskCaptor;
    private AtomicReference<NodeInfo<?>> infoRef;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        addr1 = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 7001);
        addr2 = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 7001);
        addr3 = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 7002);
    }

    @Before
    public void beforeTest()
    {
        reset(persistence, executor, promise);
        nodes = new Nodes(persistence, executor);
        infoRef = new AtomicReference<>();
        taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(executor.submit(taskCaptor.capture())).thenAnswer(inv -> promise);
    }

    @After
    public void afterTest()
    {
        verify(persistence, atMostOnce()).loadLocal();
        verify(persistence, atMostOnce()).loadPeers();
        verifyNoMoreInteractions(executor, persistence, promise);
    }

    @Test
    public void getEmptyLocal()
    {
        assertThat(nodes.getLocal().get()).isEqualTo(new LocalInfo());
    }

    @Test
    public void getEmptyPeer()
    {
        assertThat(nodes.getPeers().get(addr1)).isNull();
        assertThat(nodes.getPeers().get(addr2)).isNull();
        assertThat(nodes.getPeers().get(addr3)).isNull();
    }

    @Test
    public void loadLocal()
    {
        when(persistence.loadLocal()).thenReturn(new LocalInfo().setHostId(newHostId));
        clearInvocations(persistence);
        nodes = new Nodes(persistence, executor);

        LocalInfo r = nodes.getLocal().get();
        verify(persistence).loadLocal();
        assertThat(r.getHostId()).isEqualTo(newHostId);
    }

    @Test
    public void loadPeers()
    {
        List<PeerInfo> peers = Arrays.asList(new PeerInfo().setPeerAddressAndPort(addr1).setHostId(id1),
                                             new PeerInfo().setPeerAddressAndPort(addr2).setHostId(id2),
                                             new PeerInfo().setPeerAddressAndPort(addr3).setHostId(id3));
        when(persistence.loadPeers()).thenReturn(peers.stream());
        clearInvocations(persistence);
        nodes = new Nodes(persistence, executor);

        Set<PeerInfo> r = nodes.getPeers().get().collect(Collectors.toSet());
        assertThat(r).containsExactlyInAnyOrderElementsOf(peers);
        verify(persistence).loadPeers();

        clearInvocations(executor);
    }

    @Test
    public void updateLocalNoChanges() throws Exception
    {
        nodes.getLocal().update(current -> current.setHostId(newHostId), false, false);
        clearInvocations(persistence, executor, promise);

        LocalInfo r = updateLocalInfo(newHostId, false, false);

        checkLiveObject(r, () -> nodes.getLocal().get(), newHostId);
    }

    @Test
    public void updateLocalWithSomeChange() throws Exception
    {
        LocalInfo r = updateLocalInfo(newHostId, false, false);

        verify(executor).submit(any(Runnable.class));

        taskCaptor.getValue().run();
        verify(persistence).saveLocal(argThat(info -> info.getHostId().equals(newHostId)));

        checkLiveObject(r, () -> nodes.getLocal().get(), newHostId);
    }

    @Test
    public void updateLocalWithSomeChangeBlocking() throws Exception
    {
        LocalInfo r = updateLocalInfo(newHostId, true, false);

        verify(executor).submit(any(Runnable.class));
        verify(promise).get();

        taskCaptor.getValue().run();
        verify(persistence).saveLocal(argThat(info -> info.getHostId().equals(newHostId)));
        verify(persistence).syncLocal();

        checkLiveObject(r, () -> nodes.getLocal().get(), newHostId);
    }

    @Test
    public void updateLocalWithForce() throws Exception
    {
        nodes.getLocal().update(current -> current.setHostId(newHostId), false, false);
        clearInvocations(persistence, executor, promise);

        LocalInfo r = updateLocalInfo(newHostId, false, true);

        verify(executor).submit(any(Runnable.class));

        taskCaptor.getValue().run();
        verify(persistence).saveLocal(argThat(info -> info.getHostId().equals(newHostId)));

        checkLiveObject(r, () -> nodes.getLocal().get(), newHostId);
    }

    @Test
    public void updatePeersNoChanges() throws Exception
    {
        nodes.getPeers().update(addr1, current -> current.setHostId(newHostId), false, false);
        clearInvocations(persistence, executor, promise);

        PeerInfo r = updatePeerInfo(newHostId, false, false);

        checkLiveObject(r, () -> nodes.getPeers().get(addr1), newHostId);
    }

    @Test
    public void updatePeersWithSomeChange() throws Exception
    {
        PeerInfo r = updatePeerInfo(newHostId, false, false);

        verify(executor).submit(any(Runnable.class));

        taskCaptor.getValue().run();
        verify(persistence).savePeer(argThat(info -> info.getHostId().equals(newHostId)));

        checkLiveObject(r, () -> nodes.getPeers().get(addr1), newHostId);
    }

    @Test
    public void updatePeersWithSomeChangeBlocking() throws Exception
    {
        PeerInfo r = updatePeerInfo(newHostId, true, false);

        verify(executor).submit(any(Runnable.class));
        verify(promise).get();

        taskCaptor.getValue().run();
        verify(persistence).savePeer(argThat(info -> info.getHostId().equals(newHostId)));
        verify(persistence).syncPeers();

        checkLiveObject(r, () -> nodes.getPeers().get(addr1), newHostId);
    }

    @Test
    public void updatePeersWithForce() throws Exception
    {
        nodes.getPeers().update(addr1, current -> current.setHostId(newHostId), false, false);
        clearInvocations(persistence, executor, promise);

        PeerInfo r = updatePeerInfo(newHostId, false, true);

        verify(executor).submit(any(Runnable.class));

        taskCaptor.getValue().run();
        verify(persistence).savePeer(argThat(info -> info.getHostId().equals(newHostId)));

        checkLiveObject(r, () -> nodes.getPeers().get(addr1), newHostId);
    }

    @Test
    public void getAllPeers()
    {
        PeerInfo p1 = nodes.getPeers().update(addr1, current -> current.setHostId(id1));
        PeerInfo p2 = nodes.getPeers().update(addr2, current -> current.setHostId(id2));
        PeerInfo p3 = nodes.getPeers().update(addr3, current -> current.setHostId(id3));

        Set<PeerInfo> peers = nodes.getPeers().get().collect(Collectors.toSet());
        assertThat(peers).containsExactlyInAnyOrder(p1, p2, p3);

        clearInvocations(executor);
    }

    @Test
    public void removePeer() throws Exception
    {
        PeerInfo p1 = nodes.getPeers().update(addr1, current -> current.setHostId(id1));
        PeerInfo p2 = nodes.getPeers().update(addr2, current -> current.setHostId(id2));

        clearInvocations(executor);

        PeerInfo r = nodes.getPeers().remove(addr2, false, true);
        assertThat(r).isEqualTo(p2.setRemoved(true));
        verify(executor).submit(any(Runnable.class));

        taskCaptor.getValue().run();
        verify(persistence).deletePeer(addr2);

        Set<PeerInfo> peers = nodes.getPeers().get().collect(Collectors.toSet());
        assertThat(peers).containsExactlyInAnyOrder(p1);
    }

    @Test
    public void removePeerBlocking() throws Exception
    {
        PeerInfo p1 = nodes.getPeers().update(addr1, current -> current.setHostId(id1));
        PeerInfo p2 = nodes.getPeers().update(addr2, current -> current.setHostId(id2));

        clearInvocations(executor);

        PeerInfo r = nodes.getPeers().remove(addr2, true, true);
        assertThat(r).isEqualTo(p2.setRemoved(true));
        verify(executor).submit(any(Runnable.class));
        verify(promise).get();

        taskCaptor.getValue().run();
        verify(persistence).deletePeer(addr2);
        verify(persistence).syncPeers();

        Set<PeerInfo> peers = nodes.getPeers().get().collect(Collectors.toSet());
        assertThat(peers).containsExactlyInAnyOrder(p1);
    }

    @Test
    public void removeMissingPeer()
    {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        PeerInfo r1 = nodes.getPeers().update(addr1, current -> current.setHostId(id1));
        PeerInfo r2 = nodes.getPeers().update(addr2, current -> current.setHostId(id2));

        clearInvocations(executor);

        PeerInfo r = nodes.getPeers().remove(addr3, false, true);
        assertThat(r).isNull();

        Set<PeerInfo> peers = nodes.getPeers().get().collect(Collectors.toSet());
        assertThat(peers).containsExactlyInAnyOrder(r1, r2);
    }

    @Test
    public void softRemovePeer()
    {
        PeerInfo p1 = nodes.getPeers().update(addr1, current -> current.setHostId(id1));
        PeerInfo p2 = nodes.getPeers().update(addr2, current -> current.setHostId(id2));

        clearInvocations(executor);

        PeerInfo r = nodes.getPeers().remove(addr2, false, false);
        p2.setRemoved(true);
        assertThat(r).isEqualTo(p2);

        verify(executor).submit(any(Runnable.class));

        taskCaptor.getValue().run();
        verify(persistence).deletePeer(addr2);

        Set<PeerInfo> peers = nodes.getPeers().get().collect(Collectors.toSet());
        assertThat(peers).containsExactlyInAnyOrder(p1, p2);
    }

    private void checkLiveObject(NodeInfo<?> r, Callable<NodeInfo<?>> currentSupplier, UUID hostId) throws Exception
    {
        assertThat(r.getHostId()).isEqualTo(hostId);
        assertThat(infoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.setHostId(UUID.randomUUID())).isNotEqualTo(currentSupplier.call()); // mutation on duplicate should not have effect on live object
        assertThat(currentSupplier.call().getHostId()).isEqualTo(hostId);
    }

    private LocalInfo updateLocalInfo(UUID hostId, boolean blocking, boolean force)
    {
        return nodes.getLocal().update(previous -> {
            infoRef.set(previous);
            previous.setHostId(hostId);
            return previous;
        }, blocking, force);
    }

    private PeerInfo updatePeerInfo(UUID hostId, boolean blocking, boolean force)
    {
        return nodes.getPeers().update(addr1, previous -> {
            infoRef.set(previous);
            previous.setHostId(hostId);
            return previous;
        }, blocking, force);
    }
}

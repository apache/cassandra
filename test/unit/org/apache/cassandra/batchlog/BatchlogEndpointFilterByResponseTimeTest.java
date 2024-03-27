package org.apache.cassandra.batchlog;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageService;


import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class BatchlogEndpointFilterByResponseTimeTest
{
  private int sufficient_gossip_history = 30; // amount of gossips that are expected to suffice in identifying nodes' response time
  private int lag_factor = 3; // amount of fast gossip completions for each slow gossip completion

  @BeforeClass
  public static void setup()
  {
    System.setProperty("cassandra.max_local_pause_in_ms", "20000"); // ref: org.apache.cassandra.batchlog.BatchlogEndpointFilter
    DatabaseDescriptor.daemonInitialization();
    DatabaseDescriptor.setPhiConvictThreshold(2); // should be high enough to not "convict" endpoints with high Phi in the tests
    CommitLog.instance.start();
  }

  @Test
  public void absenceOfPhiDoesNotBreakCodeExecutionTest() throws UnknownHostException
  {
    List<InetAddressAndPort> endpoints = new ArrayList<>();

    Util.createInitialRing(StorageService.instance, new RandomPartitioner(), new ArrayList<>(), new ArrayList<>(), endpoints, new ArrayList<>(), 5);

    Multimap<String, InetAddressAndPort> rackToEndpoints = HashMultimap.create(1, 5);
    rackToEndpoints.putAll("local", endpoints);
    
    Collection<InetAddressAndPort> filteredEndpoints = ReplicaPlans.filterBatchlogEndpoints(
      "local",
      rackToEndpoints,
      FailureDetector.instance::sortEndpointsByResponseTime,
      FailureDetector.isEpASlowerThanEpB,
      FailureDetector.isEndpointAlive
    );

    assertTrue(filteredEndpoints.size() == 2);
  }
  
  @Test
  public void singleRackForReplicationTest() throws UnknownHostException
  {
    List<InetAddressAndPort> endpoints = new ArrayList<>();

    Util.createInitialRing(StorageService.instance, new RandomPartitioner(), new ArrayList<>(), new ArrayList<>(), endpoints, new ArrayList<>(), 5);

    for (int gossip_counter = 0; gossip_counter < sufficient_gossip_history; gossip_counter++)
    {
      FailureDetector.instance.report(endpoints.get(1));
      FailureDetector.instance.report(endpoints.get(3));
      if (gossip_counter % lag_factor == 0)
      {
        FailureDetector.instance.report(endpoints.get(2));
        FailureDetector.instance.report(endpoints.get(4));
      }
      for (int idx = 1; idx < endpoints.size(); idx++)
        FailureDetector.instance.interpret(endpoints.get(idx));
    }
    
    Multimap<String, InetAddressAndPort> rackToEndpoints = HashMultimap.create(1, 5);
    rackToEndpoints.putAll("local", endpoints);
    
    Collection<InetAddressAndPort> filteredEndpoints = ReplicaPlans.filterBatchlogEndpoints(
      "local",
      rackToEndpoints,
      FailureDetector.instance::sortEndpointsByResponseTime,
      FailureDetector.isEpASlowerThanEpB,
      FailureDetector.isEndpointAlive
    );

    assertTrue(filteredEndpoints.size() == 2);
    assertTrue(filteredEndpoints.contains(endpoints.get(1)));
    assertTrue(filteredEndpoints.contains(endpoints.get(3)));
  }

  @Test
  public void aCoupleOfRacksForReplicationTest() throws UnknownHostException
  {
    int nodes_per_rack = 2;
    List<String> racks = new ArrayList<String>();
    racks.add("local");
    racks.add("rack_a");
    racks.add("rack_b");
    racks.add("rack_c");
    List<InetAddressAndPort> endpoints = new ArrayList<>();

    Util.createInitialRing(
      StorageService.instance,
      new RandomPartitioner(),
      new ArrayList<>(),
      new ArrayList<>(),
      endpoints,
      new ArrayList<>(),
      nodes_per_rack * racks.size()
    );

    for (int gossip_counter = 0; gossip_counter < sufficient_gossip_history; gossip_counter++)
    {
      FailureDetector.instance.report(endpoints.get(3));
      FailureDetector.instance.report(endpoints.get(4));
      if (gossip_counter % lag_factor == 0)
      {
        FailureDetector.instance.report(endpoints.get(1));
        FailureDetector.instance.report(endpoints.get(2));
        FailureDetector.instance.report(endpoints.get(5));
        FailureDetector.instance.report(endpoints.get(6));
        FailureDetector.instance.report(endpoints.get(7));
      }
      for (int idx = 1; idx < endpoints.size(); idx++)
        FailureDetector.instance.interpret(endpoints.get(idx));
    }
    
    Multimap<String, InetAddressAndPort> racksToEndpoints = HashMultimap.create(racks.size(), nodes_per_rack);
    for (int i = 0; i < racks.size(); i++)
      racksToEndpoints.putAll(racks.get(i), endpoints.subList(2 * i, 2 * i + 2));
    
    Collection<InetAddressAndPort> filteredEndpoints = ReplicaPlans.filterBatchlogEndpoints(
      racks.get(0),
      racksToEndpoints,
      FailureDetector.instance::sortEndpointsByResponseTime,
      FailureDetector.isEpASlowerThanEpB,
      FailureDetector.isEndpointAlive
    );

    assertTrue(filteredEndpoints.size() == 2);
    assertTrue(filteredEndpoints.contains(endpoints.get(3)));
    assertTrue(filteredEndpoints.contains(endpoints.get(4)));
  }
}

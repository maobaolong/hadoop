/**
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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.net.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Manage datanodes, include decommission and other activities.
 */
@InterfaceStability.Evolving
public interface DatanodeManager {

  void activate(final Configuration conf);

  void close();

  /** @return the network topology. */
  NetworkTopology getNetworkTopology();

  /** @return the heartbeat manager. */
  HeartbeatManager getHeartbeatManager();

  DatanodeAdminManager getDatanodeAdminManager();

  HostConfigManager getHostConfigManager();

  void setHeartbeatExpireInterval(long expiryMs);

  FSClusterStats getFSClusterStats();

  int getBlockInvalidateLimit();

  /** @return the datanode statistics. */
  DatanodeStatistics getDatanodeStatistics();

  /**
   * Sort the non-striped located blocks by the distance to the target host.
   *
   * For striped blocks, it will only move decommissioned/stale nodes to the
   * bottom. For example, assume we have storage list:
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9
   * mapping to block indices:
   * 0, 1, 2, 3, 4, 5, 6, 7, 8, 2
   *
   * Here the internal block b2 is duplicated, locating in d2 and d9. If d2 is
   * a decommissioning node then should switch d2 and d9 in the storage list.
   * After sorting locations, will update corresponding block indices
   * and block tokens.
   */
  void sortLocatedBlocks(final String targetHost,
      final List<LocatedBlock> locatedBlocks);

  /** @return the datanode descriptor for the host. */
  DatanodeDescriptor getDatanodeByHost(final String host);

  /** @return the datanode descriptor for the host. */
  DatanodeDescriptor getDatanodeByXferAddr(String host, int xferPort);

  /** @return the datanode descriptors for all nodes. */
  Set<DatanodeDescriptor> getDatanodes();

  /** @return the Host2NodesMap */
  Host2NodesMap getHost2DatanodeMap();

  /**
   * Given datanode address or host name, returns the DatanodeDescriptor for the
   * same, or if it doesn't find the datanode, it looks for a machine local and
   * then rack local datanode, if a rack local datanode is not possible either,
   * it returns the DatanodeDescriptor of any random node in the cluster.
   *
   * @param address hostaddress:transfer address
   * @return the best match for the given datanode
   */
  DatanodeDescriptor getDatanodeDescriptor(String address);

  /** Get a datanode descriptor given corresponding DatanodeUUID */
  DatanodeDescriptor getDatanode(final String datanodeUuid);

  /**
   * Get data node by datanode ID.
   * 
   * @param nodeID datanode ID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws UnregisteredNodeException
   */
  DatanodeDescriptor getDatanode(DatanodeID nodeID)
      throws UnregisteredNodeException;

  DatanodeStorageInfo[] getDatanodeStorageInfos(
      DatanodeID[] datanodeID, String[] storageIDs,
      String format, Object... args) throws UnregisteredNodeException;

  /**
   * Remove a datanode
   * @throws UnregisteredNodeException 
   */
  void removeDatanode(final DatanodeID node)
      throws UnregisteredNodeException;

  /** Remove a dead datanode. */
  void removeDeadDatanode(final DatanodeID nodeID,
      boolean removeBlocksFromBlockMap);

  /** Is the datanode dead? */
  boolean isDatanodeDead(DatanodeDescriptor node);

  /** Add a datanode. */
  void addDatanode(final DatanodeDescriptor node);

  HashMap<String, Integer> getDatanodesSoftwareVersions();

  /**
   * Resolve network locations for specified hosts
   *
   * @return Network locations if available, Else returns null
   */
  List<String> resolveNetworkLocation(List<String> names);

  /**
   * Register the given datanode with the namenode. NB: the given
   * registration is mutated and given back to the datanode.
   *
   * @param nodeReg the datanode registration
   * @throws DisallowedDatanodeException if the registration request is
   *    denied because the datanode does not match includes/excludes
   * @throws UnresolvedTopologyException if the registration request is 
   *    denied because resolving datanode network location fails.
   */
  void registerDatanode(DatanodeRegistration nodeReg)
      throws DisallowedDatanodeException, UnresolvedTopologyException;
  /**
   * Rereads conf to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   */
  void refreshNodes(final Configuration conf) throws IOException;

  /** @return the number of live datanodes. */
  int getNumLiveDataNodes();

  /** @return the number of dead datanodes. */
  int getNumDeadDataNodes();

  /** @return list of datanodes where decommissioning is in progress. */
  List<DatanodeDescriptor> getDecommissioningNodes();

  /** @return list of datanodes that are entering maintenance. */
  List<DatanodeDescriptor> getEnteringMaintenanceNodes();

  /* Getter and Setter for stale DataNodes related attributes */

  long getBlocksPerPostponedMisreplicatedBlocksRescan();

  /**
   * @return The time interval used to mark DataNodes as stale.
   */
  long getStaleInterval();

  long getHeartbeatInterval();

  long getHeartbeatRecheckInterval();

  boolean shouldAvoidStaleDataNodesForWrite();

  /**
   * Set the number of current stale DataNodes. The HeartbeatManager got this
   * number based on DataNodes' heartbeats.
   * 
   * @param numStaleNodes
   *          The number of stale DataNodes to be set.
   */
  void setNumStaleNodes(int numStaleNodes);
  
  /**
   * @return Return the current number of stale DataNodes (detected by
   * HeartbeatManager). 
   */
  int getNumStaleNodes();

  /**
   * Get the number of content stale storages.
   */
  int getNumStaleStorages();

  /**
   * Set the number of content stale storages.
   *
   * @param numStaleStorages The number of content stale storages.
   */
  void setNumStaleStorages(int numStaleStorages);

  /** Fetch live and dead datanodes. */
  void fetchDatanodes(final List<DatanodeDescriptor> live,
      final List<DatanodeDescriptor> dead, final boolean removeDecommissionNode);


  /** For generating datanode reports */
  List<DatanodeDescriptor> getDatanodeListForReport(
      final DatanodeReportType type);

  SlowDiskTracker getSlowDiskTracker();
  /**
   * Retrieve information about slow disks as a JSON.
   * Returns null if we are not tracking slow disks.
   * @return
   */
  String getSlowDisksReport();

  /** Handle heartbeat from datanodes. */
  DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, final String blockPoolId,
      long cacheCapacity, long cacheUsed, int xceiverCount, 
      int maxTransfers, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      @Nonnull SlowPeerReports slowPeers,
      @Nonnull SlowDiskReports slowDisks) throws IOException;

  /**
   * Handles a lifeline message sent by a DataNode.
   *
   * @param nodeReg registration info for DataNode sending the lifeline
   * @param reports storage reports from DataNode
   * @param blockPoolId block pool ID
   * @param cacheCapacity cache capacity at DataNode
   * @param cacheUsed cache used at DataNode
   * @param xceiverCount estimated count of transfer threads running at DataNode
   * @param maxTransfers count of transfers running at DataNode
   * @param failedVolumes count of failed volumes at DataNode
   * @param volumeFailureSummary info on failed volumes at DataNode
   * @throws IOException if there is an error
   */
  void handleLifeline(DatanodeRegistration nodeReg,
      StorageReport[] reports, String blockPoolId, long cacheCapacity,
      long cacheUsed, int xceiverCount, int maxTransfers, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) throws IOException;

  void markAllDatanodesStale();

  /**
   * Clear any actions that are queued up to be sent to the DNs
   * on their next heartbeats. This includes block invalidations,
   * recoveries, and replication requests.
   */
  void clearPendingQueues();

  void clearPendingCachingCommands();

  /**
   * Reset the lastCachingDirectiveSentTimeMs field of all the DataNodes we
   * know about.
   */
  void resetLastCachingDirectiveSentTime();

  void setShouldSendCachingCommands(boolean shouldSendCachingCommands);

  void setHeartbeatInterval(long intervalSeconds);

  void setHeartbeatRecheckInterval(int recheckInterval);

  /**
   * Retrieve information about slow peers as a JSON.
   * Returns null if we are not tracking slow peers.
   * @return
   */
  String getSlowPeersReport();

  /**
   * Generates datanode reports for the given report type.
   *
   * @param type
   *          type of the datanode report
   * @return array of DatanodeStorageReports
   */
  DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type);

  /** Prints information about all datanodes. */
  void datanodeDump(final PrintWriter out);


  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.datanode.balance.bandwidthPerSec.
   *
   * A system administrator can tune the balancer bandwidth parameter
   * (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
   * "dfsadmin -setBalanacerBandwidth newbandwidth", at which point the
   * following 'bandwidth' variable gets updated with the new value for each
   * node. Once the heartbeat command is issued to update the value on the
   * specified datanode, this value will be set back to 0.
   *
   * @param bandwidth Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  void setBalancerBandwidth(long bandwidth) throws IOException;
}


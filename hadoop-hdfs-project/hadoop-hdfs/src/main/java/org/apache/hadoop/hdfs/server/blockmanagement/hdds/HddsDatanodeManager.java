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
package org.apache.hadoop.hdfs.server.blockmanagement.hdds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.FSClusterStats;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.blockmanagement.Host2NodesMap;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowDiskTracker;
import org.apache.hadoop.hdfs.server.blockmanagement.UnresolvedTopologyException;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.net.NetworkTopology;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HddsDatanodeManager implements DatanodeManager {
  private final HeartbeatManager heartbeatManager;
  private final ArrayList<DatanodeDescriptor> nodes = new ArrayList<>();

  public HddsDatanodeManager(final Namesystem namesystem,
      final BlockManager blockManager, final Configuration conf) {
    heartbeatManager = new HddsHeartbeatManager(namesystem, blockManager, conf);
  }

  @Override
  public void activate(Configuration conf) {
  }

  @Override
  public void close() {

  }

  /**
   * @return the network topology.
   */
  @Override
  public NetworkTopology getNetworkTopology() {
    return null;
  }

  /**
   * @return the heartbeat manager.
   */
  @Override
  public HeartbeatManager getHeartbeatManager() {
    return heartbeatManager;
  }

  @Override
  public DatanodeAdminManager getDatanodeAdminManager() {
    return null;
  }

  @Override
  public HostConfigManager getHostConfigManager() {
    return null;
  }

  @Override
  public void setHeartbeatExpireInterval(long expiryMs) {

  }

  @Override
  public FSClusterStats getFSClusterStats() {
    return null;
  }

  @Override
  public int getBlockInvalidateLimit() {
    return 0;
  }

  /**
   * @return the datanode statistics.
   */
  @Override
  public DatanodeStatistics getDatanodeStatistics() {
    return heartbeatManager;
  }

  /**
   * Sort the non-striped located blocks by the distance to the target host.
   * <p>
   * For striped blocks, it will only move decommissioned/stale nodes to the
   * bottom. For example, assume we have storage list:
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9
   * mapping to block indices:
   * 0, 1, 2, 3, 4, 5, 6, 7, 8, 2
   * <p>
   * Here the internal block b2 is duplicated, locating in d2 and d9. If d2 is
   * a decommissioning node then should switch d2 and d9 in the storage list.
   * After sorting locations, will update corresponding block indices
   * and block tokens.
   *
   * @param targetHost
   * @param locatedBlocks
   */
  @Override
  public void sortLocatedBlocks(String targetHost,
                                List<LocatedBlock> locatedBlocks) {

  }

  /**
   * @param host
   * @return the datanode descriptor for the host.
   */
  @Override
  public DatanodeDescriptor getDatanodeByHost(String host) {
    return null;
  }

  /**
   * @param host
   * @param xferPort
   * @return the datanode descriptor for the host.
   */
  @Override
  public DatanodeDescriptor getDatanodeByXferAddr(String host, int xferPort) {
    return null;
  }

  /**
   * @return the datanode descriptors for all nodes.
   */
  @Override
  public Set<DatanodeDescriptor> getDatanodes() {
    return new HashSet<>(nodes);
  }

  /**
   * @return the Host2NodesMap
   */
  @Override
  public Host2NodesMap getHost2DatanodeMap() {
    return null;
  }

  /**
   * Given datanode address or host name, returns the DatanodeDescriptor for the
   * same, or if it doesn't find the datanode, it looks for a machine local and
   * then rack local datanode, if a rack local datanode is not possible either,
   * it returns the DatanodeDescriptor of any random node in the cluster.
   *
   * @param address hostaddress:transfer address
   * @return the best match for the given datanode
   */
  @Override
  public DatanodeDescriptor getDatanodeDescriptor(String address) {
    return null;
  }

  /**
   * Get a datanode descriptor given corresponding DatanodeUUID
   *
   * @param datanodeUuid
   */
  @Override
  public DatanodeDescriptor getDatanode(String datanodeUuid) {
    return null;
  }

  /**
   * Get data node by datanode ID.
   *
   * @param nodeID datanode ID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws UnregisteredNodeException
   */
  @Override
  public DatanodeDescriptor getDatanode(DatanodeID nodeID)
      throws UnregisteredNodeException {
    return null;
  }

  @Override
  public DatanodeStorageInfo[] getDatanodeStorageInfos(DatanodeID[] datanodeID,
      String[] storageIDs, String format, Object... args)
      throws UnregisteredNodeException {
    return new DatanodeStorageInfo[0];
  }

  /**
   * Remove a datanode
   *
   * @param node
   * @throws UnregisteredNodeException
   */
  @Override
  public void removeDatanode(DatanodeID node) throws UnregisteredNodeException {

  }

  /**
   * Remove a dead datanode.
   *
   * @param nodeID
   * @param removeBlocksFromBlockMap
   */
  @Override
  public void removeDeadDatanode(DatanodeID nodeID,
      boolean removeBlocksFromBlockMap) {

  }

  /**
   * Is the datanode dead?
   *
   * @param node
   */
  @Override
  public boolean isDatanodeDead(DatanodeDescriptor node) {
    return false;
  }

  /**
   * Add a datanode.
   *
   * @param node
   */
  @Override
  public void addDatanode(DatanodeDescriptor node) {

  }

  @Override
  public HashMap<String, Integer> getDatanodesSoftwareVersions() {
    return new HashMap<>();
  }

  /**
   * Resolve network locations for specified hosts
   *
   * @param names
   * @return Network locations if available, Else returns null
   */
  @Override
  public List<String> resolveNetworkLocation(List<String> names) {
    return null;
  }

  /**
   * Register the given datanode with the namenode. NB: the given
   * registration is mutated and given back to the datanode.
   *
   * @param nodeReg the datanode registration
   * @throws DisallowedDatanodeException if the registration request is
   *                                     denied because the datanode does not match includes/excludes
   * @throws UnresolvedTopologyException if the registration request is
   *                                     denied because resolving datanode network location fails.
   */
  @Override
  public void registerDatanode(DatanodeRegistration nodeReg) throws
      DisallowedDatanodeException, UnresolvedTopologyException {

  }

  /**
   * Rereads conf to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   *
   * @param conf
   */
  @Override
  public void refreshNodes(Configuration conf) throws IOException {

  }

  /**
   * @return the number of live datanodes.
   */
  @Override
  public int getNumLiveDataNodes() {
    return 0;
  }

  /**
   * @return the number of dead datanodes.
   */
  @Override
  public int getNumDeadDataNodes() {
    return 0;
  }

  /**
   * @return list of datanodes where decommissioning is in progress.
   */
  @Override
  public List<DatanodeDescriptor> getDecommissioningNodes() {
    return nodes;
  }

  /**
   * @return list of datanodes that are entering maintenance.
   */
  @Override
  public List<DatanodeDescriptor> getEnteringMaintenanceNodes() {
    return nodes;
  }

  @Override
  public long getBlocksPerPostponedMisreplicatedBlocksRescan() {
    return 0;
  }

  /**
   * @return The time interval used to mark DataNodes as stale.
   */
  @Override
  public long getStaleInterval() {
    return 0;
  }

  @Override
  public long getHeartbeatInterval() {
    return 0;
  }

  @Override
  public long getHeartbeatRecheckInterval() {
    return 0;
  }

  @Override
  public boolean shouldAvoidStaleDataNodesForWrite() {
    return false;
  }

  /**
   * Set the number of current stale DataNodes. The HeartbeatManager got this
   * number based on DataNodes' heartbeats.
   *
   * @param numStaleNodes The number of stale DataNodes to be set.
   */
  @Override
  public void setNumStaleNodes(int numStaleNodes) {

  }

  /**
   * @return Return the current number of stale DataNodes (detected by
   * HeartbeatManager).
   */
  @Override
  public int getNumStaleNodes() {
    return 0;
  }

  /**
   * Get the number of content stale storages.
   */
  @Override
  public int getNumStaleStorages() {
    return 0;
  }

  /**
   * Set the number of content stale storages.
   *
   * @param numStaleStorages The number of content stale storages.
   */
  @Override
  public void setNumStaleStorages(int numStaleStorages) {

  }

  /**
   * Fetch live and dead datanodes.
   *
   * @param live
   * @param dead
   * @param removeDecommissionNode
   */
  @Override
  public void fetchDatanodes(List<DatanodeDescriptor> live,
      List<DatanodeDescriptor> dead, boolean removeDecommissionNode) {

  }

  /**
   * For generating datanode reports
   *
   * @param type
   */
  @Override
  public List<DatanodeDescriptor> getDatanodeListForReport(
      HdfsConstants.DatanodeReportType type) {
    return null;
  }

  @Override
  public SlowDiskTracker getSlowDiskTracker() {
    return null;
  }

  /**
   * Retrieve information about slow disks as a JSON.
   * Returns null if we are not tracking slow disks.
   *
   * @return
   */
  @Override
  public String getSlowDisksReport() {
    return null;
  }

  /**
   * Handle heartbeat from datanodes.
   *
   * @param nodeReg
   * @param reports
   * @param blockPoolId
   * @param cacheCapacity
   * @param cacheUsed
   * @param xceiverCount
   * @param maxTransfers
   * @param failedVolumes
   * @param volumeFailureSummary
   * @param slowPeers
   * @param slowDisks
   */
  @Override
  public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, String blockPoolId, long cacheCapacity,
      long cacheUsed, int xceiverCount, int maxTransfers, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      @Nonnull SlowPeerReports slowPeers, @Nonnull SlowDiskReports slowDisks)
      throws IOException {
    return new DatanodeCommand[0];
  }

  /**
   * Handles a lifeline message sent by a DataNode.
   *
   * @param nodeReg              registration info for DataNode sending the lifeline
   * @param reports              storage reports from DataNode
   * @param blockPoolId          block pool ID
   * @param cacheCapacity        cache capacity at DataNode
   * @param cacheUsed            cache used at DataNode
   * @param xceiverCount         estimated count of transfer threads running at DataNode
   * @param maxTransfers         count of transfers running at DataNode
   * @param failedVolumes        count of failed volumes at DataNode
   * @param volumeFailureSummary info on failed volumes at DataNode
   * @throws IOException if there is an error
   */
  @Override
  public void handleLifeline(DatanodeRegistration nodeReg,
      StorageReport[] reports, String blockPoolId, long cacheCapacity,
      long cacheUsed, int xceiverCount, int maxTransfers, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) throws IOException {

  }

  @Override
  public void markAllDatanodesStale() {

  }

  /**
   * Clear any actions that are queued up to be sent to the DNs
   * on their next heartbeats. This includes block invalidations,
   * recoveries, and replication requests.
   */
  @Override
  public void clearPendingQueues() {

  }

  @Override
  public void clearPendingCachingCommands() {

  }

  /**
   * Reset the lastCachingDirectiveSentTimeMs field of all the DataNodes we
   * know about.
   */
  @Override
  public void resetLastCachingDirectiveSentTime() {

  }

  @Override
  public void setShouldSendCachingCommands(boolean shouldSendCachingCommands) {

  }

  @Override
  public void setHeartbeatInterval(long intervalSeconds) {

  }

  @Override
  public void setHeartbeatRecheckInterval(int recheckInterval) {

  }

  /**
   * Retrieve information about slow peers as a JSON.
   * Returns null if we are not tracking slow peers.
   *
   * @return
   */
  @Override
  public String getSlowPeersReport() {
    return null;
  }

  /**
   * Generates datanode reports for the given report type.
   *
   * @param type type of the datanode report
   * @return array of DatanodeStorageReports
   */
  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) {
    return new DatanodeStorageReport[0];
  }

  /**
   * Prints information about all datanodes.
   *
   * @param out
   */
  @Override
  public void datanodeDump(PrintWriter out) {

  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.datanode.balance.bandwidthPerSec.
   * <p>
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
  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {

  }
}

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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStats;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.blockmanagement.StorageTypeStats;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Manage the heartbeats received from datanodes.
 * The datanode list and statistics are synchronized
 * by the heartbeat manager lock.
 */
class HddsHeartbeatManager implements HeartbeatManager {
  static final Logger LOG = LoggerFactory.getLogger(HddsHeartbeatManager.class);

  /**
   * Stores a subset of the datanodeMap in DatanodeManager,
   * containing nodes that are considered alive.
   * The HeartbeatMonitor periodically checks for out-dated entries,
   * and removes them from the list.
   * It is synchronized by the heartbeat manager lock.
   */
  private final List<DatanodeDescriptor> datanodes = new ArrayList<>();

  /** Statistics, which are synchronized by the heartbeat manager lock. */
  private final DatanodeStats stats = new DatanodeStats();

  final Namesystem namesystem;
  final BlockManager blockManager;

  HddsHeartbeatManager(final Namesystem namesystem,
      final BlockManager blockManager, final Configuration conf) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
  }

  public void activate() {
  }

  public void close() {
  }
  
  public synchronized int getLiveDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public long getCapacityTotal() {
    return stats.getCapacityTotal();
  }

  @Override
  public long getCapacityUsed() {
    return stats.getCapacityUsed();
  }

  @Override
  public float getCapacityUsedPercent() {
    return stats.getCapacityUsedPercent();
  }

  @Override
  public long getCapacityRemaining() {
    return stats.getCapacityRemaining();
  }

  @Override
  public float getCapacityRemainingPercent() {
    return stats.getCapacityRemainingPercent();
  }

  @Override
  public long getBlockPoolUsed() {
    return stats.getBlockPoolUsed();
  }

  @Override
  public float getPercentBlockPoolUsed() {
    return stats.getPercentBlockPoolUsed();
  }

  @Override
  public long getCapacityUsedNonDFS() {
    return stats.getCapacityUsedNonDFS();
  }

  @Override
  public int getXceiverCount() {
    return stats.getXceiverCount();
  }
  
  @Override
  public int getInServiceXceiverCount() {
    return stats.getNodesInServiceXceiverCount();
  }
  
  @Override
  public int getNumDatanodesInService() {
    return stats.getNodesInService();
  }
  
  @Override
  public long getCacheCapacity() {
    return stats.getCacheCapacity();
  }

  @Override
  public long getCacheUsed() {
    return stats.getCacheUsed();
  }

  @Override
  public synchronized long[] getStats() {
    return new long[] {getCapacityTotal(),
                       getCapacityUsed(),
                       getCapacityRemaining(),
                       -1L,
                       -1L,
                       -1L,
                       -1L,
                       -1L,
                       -1L};
  }

  @Override
  public int getExpiredHeartbeats() {
    return stats.getExpiredHeartbeats();
  }

  @Override
  public Map<StorageType, StorageTypeStats> getStorageTypeStats() {
    return stats.getStatsMap();
  }

  @Override
  public long getProvidedCapacity() {
    return blockManager.getProvidedCapacity();
  }

  public synchronized void register(final DatanodeDescriptor d) {
  }

  synchronized DatanodeDescriptor[] getDatanodes() {
    return datanodes.toArray(new DatanodeDescriptor[datanodes.size()]);
  }

  public synchronized void addDatanode(final DatanodeDescriptor d) {
    // update in-service node count
    datanodes.add(d);
    d.setAlive(true);
  }

  public void updateDnStat(final DatanodeDescriptor d){
    stats.add(d);
  }

  public synchronized void removeDatanode(DatanodeDescriptor node) {
  }

  public synchronized void updateHeartbeat(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
  }

  public synchronized void updateLifeline(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
  }

  public synchronized void startDecommission(final DatanodeDescriptor node) {
  }

  public synchronized void startMaintenance(final DatanodeDescriptor node) {
  }

  public synchronized void stopMaintenance(final DatanodeDescriptor node) {
  }

  public synchronized void stopDecommission(final DatanodeDescriptor node) {
  }

  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   * Check if there are any failed storage and if so,
   * Remove all the blocks on the storage. It also covers the following less
   * common scenarios. After DatanodeStorage is marked FAILED, it is still
   * possible to receive IBR for this storage.
   * 1) DN could deliver IBR for failed storage due to its implementation.
   *    a) DN queues a pending IBR request.
   *    b) The storage of the block fails.
   *    c) DN first sends HB, NN will mark the storage FAILED.
   *    d) DN then sends the pending IBR request.
   * 2) SBN processes block request from pendingDNMessages.
   *    It is possible to have messages in pendingDNMessages that refer
   *    to some failed storage.
   *    a) SBN receives a IBR and put it in pendingDNMessages.
   *    b) The storage of the block fails.
   *    c) Edit log replay get the IBR from pendingDNMessages.
   * Alternatively, we can resolve these scenarios with the following approaches.
   * A. Make sure DN don't deliver IBR for failed storage.
   * B. Remove all blocks in PendingDataNodeMessages for the failed storage
   *    when we remove all blocks from BlocksMap for that storage.
   */
  @VisibleForTesting
  public void heartbeatCheck() {
  }
}

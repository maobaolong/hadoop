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

import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

/**
 * Manage the heartbeats received from datanodes.
 * The datanode list and statistics are synchronized
 * by the heartbeat manager lock.
 */
public interface HeartbeatManager extends DatanodeStatistics{

  void activate();

  void close();

  void register(final DatanodeDescriptor d);

  void addDatanode(final DatanodeDescriptor d);

  void updateDnStat(final DatanodeDescriptor d);

  void removeDatanode(DatanodeDescriptor node);

  int getLiveDatanodeCount();

  void updateHeartbeat(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary);

  void updateLifeline(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary);

  void startDecommission(final DatanodeDescriptor node);

  void stopDecommission(final DatanodeDescriptor node);

  void startMaintenance(final DatanodeDescriptor node);

  void stopMaintenance(final DatanodeDescriptor node);

  // for test
  void heartbeatCheck();
}

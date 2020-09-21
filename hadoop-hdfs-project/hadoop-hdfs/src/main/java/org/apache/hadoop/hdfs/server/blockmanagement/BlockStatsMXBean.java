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

import java.util.Map;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.metrics2.annotation.Metric;

/**
 * This is an interface used to retrieve statistic information related to
 * block management.
 */
public interface BlockStatsMXBean {

  /**
   * The statistics of storage types.
   *
   * @return get storage statistics per storage type
   */
  Map<StorageType, StorageTypeStats> getStorageTypeStats();

  /**
   * Gets the total numbers of blocks on the cluster.
   *
   * @return the total number of blocks of the cluster
   */
  long getTotalBlocks();

  int getActiveBlockCount();

  /**
   * @return the size of UnderReplicatedBlocks
   */
  int numOfUnderReplicatedBlocks();

  long getHighestPriorityReplicatedBlockCount();

  long getHighestPriorityECBlockCount();

  int getCapacity();

  long getProvidedCapacity();

  /**
   * Get aggregated count of all blocks with low redundancy.
   */
  long getLowRedundancyBlocksCount();

  /** Returns number of blocks with corrupt replicas */
  long getCorruptReplicaBlocksCount();


  long getScheduledReplicationBlocksCount();

  long getPendingDeletionBlocksCount();

  long getMissingBlocksCount();

  long getExcessBlocksCount();

  long getMissingReplOneBlocksCount();

  long getNumTimedOutPendingReconstructions();

  /**
   * Get aggregated count of all blocks pending to be reconstructed.
   */
  long getPendingReconstructionBlocksCount();

  long getStartupDelayBlockDeletionInMs();

  int getUnderReplicatedNotMissingBlocks();

  // HA-only metric
  long getPostponedMisreplicatedBlocksCount();

  // HA-only metric
  int getPendingDataNodeMessageCount();
}

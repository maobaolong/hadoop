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

import org.apache.hadoop.hdds.scm.protocol.ScmClient;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.SafeModeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HddsSafeModeManager implements SafeModeManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(HddsSafeModeManager.class);
  ScmClient scmClient;

  public HddsSafeModeManager(ScmClient scmClient) {
    this.scmClient = scmClient;
  }

  /**
   * Initialize the safe mode information.
   *
   * @param total initial total blocks
   */
  @Override
  public void activate(long total) {

  }

  /**
   * Stop and clean up safe mode manager.
   */
  @Override
  public void close() {

  }

  /**
   * @return true if it stays in start up safe mode else false.
   */
  @Override
  public boolean isInSafeMode() {
    try {
      return scmClient.getContainerClient().inSafeMode();
    } catch (IOException e) {
      LOG.error("Failed to get SCM safe mode status", e);
      return true;
    }
  }

  /**
   * The transition of the safe mode state machine.
   * If safe mode is not currently on, this is a no-op.
   */
  @Override
  public void checkSafeMode() {

  }

  /**
   * Get insight of safe mode process.
   */
  @Override
  public String getSafeModeTip() {
    return null;
  }

  /**
   * Leave start up safe mode.
   *
   * @param force - true to force exit
   * @return true if it leaves safe mode successfully else false
   */
  @Override
  public boolean leaveSafeMode(boolean force) {
    try {
      return scmClient.getContainerClient().forceExitSafeMode();
    } catch (IOException e) {
      LOG.error("Failed to get SCM safe mode status", e);
      return false;
    }
  }

  /**
   * Adjust the total number of blocks safe and expected during safe mode.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param deltaSafe  the change in number of safe blocks
   * @param deltaTotal the change in number of total blocks expected
   */
  @Override
  public void adjustBlockTotals(int deltaSafe, int deltaTotal) {

  }

  /**
   * Set total number of blocks.
   *
   * @param total
   */
  @Override
  public void setBlockTotal(long total) {

  }

  /**
   * Increment number of safe blocks if the current block is contiguous
   * and it has reached minimal replication or
   * if the current block is striped and the number of its actual data blocks
   * reaches the number of data units specified by the erasure coding policy.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param storageNum  current number of replicas or number of internal blocks
   *                    of a striped block group
   * @param storedBlock current storedBlock which is either a
   */
  @Override
  public void incrementSafeBlockCount(int storageNum, BlockInfo storedBlock) {

  }

  /**
   * Decrement number of safe blocks if the current block is contiguous
   * and it has just fallen below minimal replication or
   * if the current block is striped and its actual data blocks has just fallen
   * below the number of data units specified by erasure coding policy.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param b
   */
  @Override
  public void decrementSafeBlockCount(BlockInfo b) {

  }

  /**
   * Check if the block report replica has a generation stamp (GS) in future.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param brr block report replica which belongs to no file in BlockManager
   */
  @Override
  public void checkBlocksWithFutureGS(BlockListAsLongs.BlockReportReplica brr) {

  }

  /**
   * Returns the number of bytes that reside in blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  @Override
  public long getBytesInFuture() {
    return 0;
  }

  /**
   * Returns the number of bytes that reside in continous blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  @Override
  public long getBytesInFutureBlocks() {
    return 0;
  }

  /**
   * Returns the number of bytes that reside in EC blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  @Override
  public long getBytesInFutureECBlockGroups() {
    return 0;
  }
}

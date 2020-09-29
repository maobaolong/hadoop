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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.metrics.ECBlockGroupsMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.ReplicatedBlocksMBean;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 * For block state management, it tries to maintain the  safety
 * property of "# of live replicas == # of expected redundancy" under
 * any events such as decommission, namenode failover, datanode failure.
 *
 * The motivation of maintenance mode is to allow admins quickly repair nodes
 * without paying the cost of decommission. Thus with maintenance mode,
 * # of live replicas doesn't have to be equal to # of expected redundancy.
 * If any of the replica is in maintenance mode, the safety property
 * is extended as follows. These property still apply for the case of zero
 * maintenance replicas, thus we can use these safe property for all scenarios.
 * a. # of live replicas >= # of min replication for maintenance.
 * b. # of live replicas <= # of expected redundancy.
 * c. # of live replicas and maintenance replicas >= # of expected redundancy.
 *
 * For regular replication, # of min live replicas for maintenance is determined
 * by DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY. This number has to <=
 * DFS_NAMENODE_REPLICATION_MIN_KEY.
 * For erasure encoding, # of min live replicas for maintenance is
 * BlockInfoStriped#getRealDataBlockNum.
 *
 * Another safety property is to satisfy the block placement policy. While the
 * policy is configurable, the replicas the policy is applied to are the live
 * replicas + maintenance replicas.
 */
@InterfaceAudience.Public
public interface BlockManager extends StoragePolicy, BlockStatsMXBean,
    ECBlockGroupsMBean, ReplicatedBlocksMBean {

  Logger LOG = LoggerFactory.getLogger(BlockManager.class);

  Logger blockLog = NameNode.blockStateChangeLog;

  BlockTokenSecretManager getBlockTokenSecretManager();

  DatanodeManager getDatanodeManager();

  StoragePolicySatisfyManager getSPSManager();

  BlockIdManager getBlockIdManager();

  SafeModeManager getSafeModeManager();

  BlockReportLeaseManager getBlockReportLeaseManager();

  void setBlockPoolId(String blockPoolId);

  String getBlockPoolId();

  ProvidedStorageMap getProvidedStorageMap();

  /*
   *  Start the service
   */
  void start(Configuration conf, long blockTotal);

  void close();

  /*
   * Shutdown the service
   */
  void shutdown();

  /*
   * Clear the service state, like reset
   */
  void clear();


  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  void clearQueues();

  /*
   * Set Block Manager property new value
   */
  Object setProperty(String property, String newValue)
      throws IllegalArgumentException;

  /*
   * Get Block Manager property new value
   */
  Object getProperty(String property);

  /** Dump meta data to out. */
  void metaSave(PrintWriter out);

  /**
   * Create SPS manager instance. It manages the user invoked sps paths and does
   * the movement.
   *
   * @param conf
   *          configuration
   * @param spsMode
   *          satisfier mode
   * @return true if the instance is successfully created, false otherwise.
   */
  boolean createSPSManager(final Configuration conf, final String spsMode);

  /**
   * Nullify SPS manager as this feature is disabled fully.
   */
  void disableSPS();

  /** @return current access keys. */
  ExportedBlockKeys getBlockKeys();

  /** Generate a block token for the located block. */
  void setBlockToken(final LocatedBlock b,
      final AccessMode mode) throws IOException;

  /**
   * Generate a data encryption key for this block pool.
   *
   * @return a data encryption key which may be used to encrypt traffic
   *         over the DataTransferProtocol
   */
  DataEncryptionKey generateDataEncryptionKey();

  boolean shouldUpdateBlockKey(final long updateTime) throws IOException;

  int getDefaultStorageNum(BlockInfo block);

  short getMinReplication();

  short getMinStorageNum(BlockInfo block);

  short getMinReplicationToBeInMaintenance();

  boolean hasMinStorage(BlockInfo block);

  boolean hasMinStorage(BlockInfo block, int liveNum);

  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum redundancy requirement
   *
   * @param bc block collection
   * @param commitBlock - contains client reported block length and generation
   * @param iip - INodes in path to bc
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock, INodesInPath iip) throws IOException;

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  void forceCompleteBlock(final BlockInfo block) throws IOException;

  /**
   * If IBR is not sent from expected locations yet, add the datanodes to
   * pendingReconstruction in order to keep RedundancyMonitor from scheduling
   * the block.
   */
  void addExpectedReplicasToPending(BlockInfo blk);

  /**
   * Convert the last block of the file to an under construction block.<p>
   * The block is converted only if the file has blocks and the last one
   * is a partial block (its size is less than the preferred block size).
   * The converted block is returned to the client.
   * The client uses the returned block locations to form the data pipeline
   * for this block.<br>
   * The methods returns null if there is no partial block at the end.
   * The client is supposed to allocate a new block with the next call.
   *
   * @param bc file
   * @param bytesToRemove num of bytes to remove from block
   * @return the last block locations if the block is partial or null otherwise
   */
  LocatedBlock convertLastBlockToUnderConstruction(
      BlockCollection bc, long bytesToRemove) throws IOException;

  /** Create a LocatedBlocks. */
  LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken,
      final boolean inSnapshot, FileEncryptionInfo feInfo,
      ErasureCodingPolicy ecPolicy)
      throws IOException;

  void addKeyUpdateCommand(final List<DatanodeCommand> cmds,
      final DatanodeDescriptor nodeinfo);

  /** Set replication for the blocks. */
  void setReplication(final short oldRepl, final short newRepl,
      final BlockInfo b);

  /**
   * Check whether the specified replication is between the minimum and the
   * maximum replication levels. True return it, otherwise return the minimum
   * or maximum accordingly
   */
  short adjustReplication(short replication);

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration and throw an exception if it's not.
   *
   * @param src the path to the target file
   * @param replication the requested replication factor
   * @param clientName the name of the client node making the request
   * @throws IOException thrown if the requested replication factor
   * is out of bounds
   */
   void verifyReplication(String src, short replication,
       String clientName) throws IOException;

  /**
   * Check if a block is replicated to at least the minimum replication.
   */
  boolean isSufficientlyReplicated(BlockInfo b);

  /** Get all blocks with location information from a datanode. */
  BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
      final long size, final long minBlockSize) throws
      UnregisteredNodeException;

  /** Remove the blocks associated to the given datanode. */
  void removeBlocksAssociatedTo(final DatanodeDescriptor node);

  /** Remove the blocks associated to the given DatanodeStorageInfo. */
  void removeBlocksAssociatedTo(final DatanodeStorageInfo storageInfo);

  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   * @param storageID if known, null otherwise.
   * @param reason a textual reason why the block should be marked corrupt,
   * for logging purposes
   */
  void findAndMarkBlockAsCorrupt(final ExtendedBlock blk, final DatanodeInfo dn,
      String storageID, String reason) throws IOException;

  /** Choose target for WebHDFS redirection. */
  DatanodeStorageInfo[] chooseTarget4WebHDFS(String src,
      DatanodeDescriptor clientnode, Set<Node> excludes, long blocksize);

  /** Choose target for getting additional datanodes for an existing pipeline. */
  DatanodeStorageInfo[] chooseTarget4AdditionalDatanode(String src,
      int numAdditionalNodes, Node clientnode, List<DatanodeStorageInfo> chosen,
      Set<Node> excludesm, long blocksize, byte storagePolicyID,
      BlockType blockType);

  /**
   * Choose target datanodes for creating a new block.
   *
   * @throws IOException
   *           if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, Node,
   *      Set, long, List, BlockStoragePolicy, EnumSet)
   */
  DatanodeStorageInfo[] chooseTarget4NewBlock(final String src,
      final int numOfReplicas, final Node client,
      final Set<Node> excludedNodes,
      final long blocksize,
      final List<String> favoredNodes,
      final byte storagePolicyID,
      final BlockType blockType,
      final ErasureCodingPolicy ecPolicy,
      final EnumSet<AddBlockFlag> flags) throws IOException;

  long requestBlockReportLeaseId(DatanodeRegistration nodeReg);

  void registerDatanode(DatanodeRegistration nodeReg) throws IOException;

  /**
   * Set the total number of blocks in the system.
   * If safe mode is not currently on, this is a no-op.
   */
  void setBlockTotal(long total);

  /**
   * Removes the blocks from blocksmap and updates the safemode blocks total.
   * @param blocks An instance of {@link BlocksMapUpdateInfo} which contains a
   *               list of blocks that need to be removed from blocksMap
   */
  void removeBlocksAndUpdateSafemodeTotal(BlocksMapUpdateInfo blocks);

  /**
   * Check block report lease.
   * @return true if lease exist and not expire
   */
  boolean checkBlockReportLease(BlockReportContext context,
      final DatanodeID nodeID) throws UnregisteredNodeException;

  /**
   * The given storage is reporting all its blocks.
   * Update the (storage-->block list) and (block-->storage list) maps.
   *
   * @return true if all known storages of the given DN have finished reporting.
   * @throws IOException
   */
  boolean processReport(final DatanodeID nodeID,
      final DatanodeStorage storage,
      final BlockListAsLongs newReport,
      BlockReportContext context) throws IOException;

  /**
   * processFirstBlockReport is intended only for processing "initial" block
   * reports, the first block report received from a DN after it registers.
   * It just adds all the valid replicas to the datanode, without calculating
   * a toRemove list (since there won't be any).  It also silently discards
   * any invalid blocks, thereby deferring their processing until
   * the next block report.
   * @param storageInfo - DatanodeStorageInfo that sent the report
   * @param report - the initial block report, to be processed
   * @throws IOException
   */
  void processFirstBlockReport(final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException;

  /**
   * The given node is reporting incremental information about some blocks.
   * This includes blocks that are starting to be received, completed being
   * received, or deleted.
   *
   * This method must be called with FSNamesystem lock held.
   */
  void processIncrementalBlockReport(final DatanodeID nodeID,
      final StorageReceivedDeletedBlocks srdb) throws IOException;

  void removeBRLeaseIfNeeded(final DatanodeID nodeID,
      final BlockReportContext context) throws IOException;

  /**
   * Mark block replicas as corrupt except those on the storages in 
   * newStorages list.
   */
  void markBlockReplicasAsCorrupt(Block oldBlock,
      BlockInfo block, long oldGenerationStamp, long oldNumBytes,
      DatanodeStorageInfo[] newStorages) throws IOException;

  /**
   * Try to process any messages that were previously queued for the given
   * block. This is called from FSEditLogLoader whenever a block's state
   * in the namespace has changed or a new block has been created.
   */
  void processQueuedMessagesForBlock(Block b) throws IOException;

  /**
   * Process any remaining queued datanode messages after entering
   * active state. At this point they will not be re-queued since
   * we are the definitive master node and thus should be up-to-date
   * with the namespace information.
   */
  void processAllPendingDNMessages() throws IOException;

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * extra or low redundancy. Place it into the respective queue.
   */
  void processMisReplicatedBlocks();

  /**
   * Get the value of whether there are any non-EC blocks using StripedID.
   *
   * @return Returns the value of whether there are any non-EC blocks using StripedID.
   */
  boolean hasNonEcBlockUsingStripedID();

  void setPostponeBlocksFromFuture(boolean postpone);

  /**
   * The given node is reporting that it received a certain block.
   */
  void addBlock(DatanodeStorageInfo storageInfo, Block block,
      String delHint) throws IOException;

  void removeBlock(BlockInfo block);

  /**
   * Return the number of nodes hosting a given block, grouped
   * by the state of those replicas.
   * For a striped block, this includes nodes storing blocks belonging to the
   * striped block group. But note we exclude duplicated internal block replicas
   * for calculating {@link NumberReplicas#liveReplicas}.
   */
  NumberReplicas countNodes(BlockInfo b);

  boolean isExcess(DatanodeDescriptor dn, BlockInfo blk);

  DatanodeStorageInfo[] getStorages(BlockInfo block);

  /** @return an iterator of the datanodes. */
  Iterable<DatanodeStorageInfo> getStorages(final Block block);

  /**
   * Get the block info of a Block.
   */
  BlockInfo getStoredBlock(Block block);

  void updateLastBlock(BlockInfo lastBlock, ExtendedBlock newBlock);

  /**
   * Check sufficient redundancy of the blocks in the collection. If any block
   * is needed reconstruction, insert it into the reconstruction queue.
   * Otherwise, if the block is more than the expected replication factor,
   * process it as an extra redundancy block.
   */
  void checkRedundancy(BlockCollection bc);

  boolean isPlacementPolicySatisfied(BlockInfo storedBlock);

  boolean isNeededReconstructionForMaintenance(BlockInfo storedBlock,
      NumberReplicas numberReplicas);

  /**
   * A block needs reconstruction if the number of redundancies is less than
   * expected or if it does not have enough racks.
   */
  boolean isNeededReconstruction(BlockInfo storedBlock,
      NumberReplicas numberReplicas);

  // Exclude maintenance, but make sure it has minimal live replicas
  // to satisfy the maintenance requirement.
  short getExpectedLiveRedundancyNum(BlockInfo block,
      NumberReplicas numberReplicas);

  BlockInfo addBlockCollection(BlockInfo block,
      BlockCollection bc);

  /**
   * Do some check when adding a block to blocksmap.
   * For HDFS-7994 to check whether then block is a NonEcBlockUsingStripedID.
   *
   */
  BlockInfo addBlockCollectionWithCheck(
      BlockInfo block, BlockCollection bc);

  /**
   * Get number of corrupt replicas of a given block.
   */
  int numCorruptReplicas(Block block);

  /**
   * Free the block from Block Manager management.
   */
  void removeBlockFromMap(BlockInfo block);

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  Iterator<BlockInfo> getCorruptReplicaBlockIterator();

  /**
   * Get the locations of replicas which are corrupt for a given block.
   */
  Collection<DatanodeDescriptor> getCorruptReplicas(Block block);

 /**
  * Get reason for certain corrupted replicas for a given block and a given dn.
  */
  String getCorruptReason(Block block, DatanodeDescriptor node);

  /**
   * Initialize replication queues.
   */
  void initializeReplQueues();

  void setInitializedReplQueues(boolean v);

  /**
   * Check if replication queues are to be populated.
   * @return true when node is HAState.Active and not in the very first safemode
   */
  boolean isPopulatingReplQueues();

  /**
   * Check if start to add replication command to replication manager
   */
  boolean shouldPopulateReplQueues();

  boolean getShouldPostponeBlocksFromFuture();

  // async processing of an action, used for IBRs.
  void enqueueBlockOp(final Runnable action) throws IOException;

  // sync batch processing for a full BR.
  <T> T runBlockOp(final Callable<T> action) throws IOException;

  /**
   * Notification of a successful block recovery.
   * @param block for which the recovery succeeded
   */
  void successfulBlockRecovery(BlockInfo block);

  /**
   * Checks whether a recovery attempt has been made for the given block.
   * If so, checks whether that attempt has timed out.
   * @param b block for which recovery is being attempted
   * @return true if no recovery attempt has been made or
   *         the previous attempt timed out
   */
  boolean addBlockRecoveryAttempt(BlockInfo b);

  // For test
  void flushBlockOps() throws IOException;
  BlockPlacementPolicy getBlockPlacementPolicy();
  void setBlockRecoveryTimeout(long blockRecoveryTimeout);

  String getBlockPoolInfo();

  static LocatedBlock newLocatedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,
      long startOffset, boolean corrupt) {
    // startOffset is unknown
    return new LocatedBlock(
        b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt,
        null);
  }

  static LocatedStripedBlock newLocatedStripedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,
      byte[] indices, long startOffset, boolean corrupt) {
    // startOffset is unknown
    return new LocatedStripedBlock(
        b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        indices, startOffset, corrupt,
        null);
  }

  static LocatedBlock newLocatedBlock(ExtendedBlock eb, BlockInfo info,
     DatanodeStorageInfo[] locs, long offset) throws IOException {
    final LocatedBlock lb;
    if (info.isStriped()) {
      lb = newLocatedStripedBlock(eb, locs,
          info.getUnderConstructionFeature().getBlockIndices(),
          offset, false);
    } else {
      lb = newLocatedBlock(eb, locs, offset, false);
    }
    return lb;
  }

  static short getExpectedRedundancyNum(BlockInfo block) {
    return block.isStriped() ?
        ((BlockInfoStriped) block).getRealTotalBlockNum() :
        block.getReplication();
  }


  /**
   * StatefulBlockInfo is used to build the "toUC" list, which is a list of
   * updates to the information about under-construction blocks.
   * Besides the block in question, it provides the ReplicaState
   * reported by the datanode in the block report.
   */
  class StatefulBlockInfo {
    final BlockInfo storedBlock; // should be UC block
    final Block reportedBlock;
    final HdfsServerConstants.ReplicaState reportedState;

    StatefulBlockInfo(BlockInfo storedBlock,
        Block reportedBlock, HdfsServerConstants.ReplicaState reportedState) {
      Preconditions.checkArgument(!storedBlock.isComplete());
      this.storedBlock = storedBlock;
      this.reportedBlock = reportedBlock;
      this.reportedState = reportedState;
    }
  }
}
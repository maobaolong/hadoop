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
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.HDDSLocatedBlock;
import org.apache.hadoop.hdds.HDDSLocatedBlocks;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.ScmClient;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.util.ChunkedArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForBlockClients;

public class HddsBlockManager implements BlockManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(HddsBlockManager.class);
  private final ScmClient scmClient;
  private final FSNamesystem namesystem;
  private final HddsDatanodeManager datanodeManager;
  private final HddsSafeModeManager safeModeManager;
  private final BlockIdManager blockIdManager;
  private String scmID;
  private String blockPoolId;
  private final BlockStoragePolicySuite storagePolicySuite;
  /** Minimum copies needed or else write is disallowed */
  public final short minReplication;
  /** The maximum number of replicas allowed for a block */
  public final short maxReplication;
  private ObjectName mxBeanName;
  private final InetSocketAddress scmBlockAddress;
  private final short minStorageNum;
  private final AtomicLong blockNum;

  public HddsBlockManager(final FSNamesystem namesystem,
      final Configuration conf) throws IOException {
    OzoneConfiguration o3Conf = OzoneConfiguration.of(conf);
    ScmBlockLocationProtocol scmBlockClient =
        ScmClient.getScmBlockClient(o3Conf);
    StorageContainerLocationProtocol scmContainerClient =
        ScmClient.getScmContainerClient(o3Conf);
    scmBlockAddress = getScmAddressForBlockClients(o3Conf);
    this.namesystem = namesystem;
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient);
    scmID = getScmID();
    datanodeManager = new HddsDatanodeManager(namesystem, this, conf);
    safeModeManager = new HddsSafeModeManager(scmClient);
    blockIdManager= new HddsBlockIdManager(this);
    storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite();
    minReplication =  (short)conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    maxReplication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY,
        DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    minStorageNum = (short)o3Conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    blockNum = new AtomicLong();
  }

  @VisibleForTesting
  public ScmClient getScmClient() {
    return scmClient;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  public String getBlockPoolInfo() {
    return "scmAddr: " + scmBlockAddress.toString() + ", scmId: " + scmID;
  }

  private String getScmID() {
    try {
      return scmClient != null && scmClient.getContainerClient() != null ?
          scmClient.getContainerClient().getScmInfo().getScmId() :
          "N/A";
    } catch (IOException e) {
      return "N/A";
    }
  }

  private void createLocatedBlockList(
      HddsLocatedBlockBuilder locatedBlocks,
      final BlockInfo[] blocks,
      final long offset, final long length,
      final BlockTokenIdentifier.AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0, blkSize = 0;
    HddsBlockInfo hddsBlockInfo = (HddsBlockInfo) blocks[0];
    int nrBlocks = (hddsBlockInfo.getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      hddsBlockInfo = (HddsBlockInfo) blocks[curBlk];
      blkSize = hddsBlockInfo.getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return;

    Set<Long> containerIDs = new HashSet<>();
    for (BlockInfo bi : blocks) {
      hddsBlockInfo = (HddsBlockInfo) bi;
      containerIDs.add(hddsBlockInfo.getContainerID());
    }
    Map<Long, ContainerWithPipeline> containerWithPipelineMap =
        this.refreshPipeline(containerIDs);

    long endOff = offset + length;
    do {
      hddsBlockInfo = (HddsBlockInfo) blocks[curBlk];
      ContainerWithPipeline cp =
          containerWithPipelineMap.get(hddsBlockInfo.getContainerID());

      locatedBlocks.addBlock(
          createLocatedBlock(locatedBlocks, hddsBlockInfo, curPos, mode, cp.getPipeline()));
      curPos += hddsBlockInfo.getNumBytes();
      curBlk++;
    } while (curPos < endOff
        && curBlk < blocks.length
        && !locatedBlocks.isBlockMax());
    return;
  }

  private HDDSLocatedBlock createLocatedBlock(
      HddsLocatedBlockBuilder locatedBlocks,
      final HddsBlockInfo blk, final long pos,
      final BlockTokenIdentifier.AccessMode mode,
      final Pipeline pipeline
  ) throws IOException {
    // TODO(baoloongmao): open block token until necessary
    HDDSLocatedBlock locationInfo = null;
    if (blk != null) {
      locationInfo = new HDDSLocatedBlock.Builder()
          .setBlockID(new BlockID(blk.getContainerID(), blk.getBlockId()))
          .setPipeline(pipeline)
          .setLength(blk.getNumBytes())
          .setOffset(pos)
          .build();
    }
    return locationInfo;
  }

  @Override
  public BlockTokenSecretManager getBlockTokenSecretManager() {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getBlockTokenSecretManager!");
  }

  @Override
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  @Override
  public StoragePolicySatisfyManager getSPSManager() {
    return null;
  }

  @Override
  public BlockIdManager getBlockIdManager() {
    return blockIdManager;
  }

  @Override
  public SafeModeManager getSafeModeManager() {
    return safeModeManager;
  }

  @Override
  public BlockReportLeaseManager getBlockReportLeaseManager() {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getBlockReportLeaseManager!");
  }

  @Override
  public void setBlockPoolId(String blockPoolId) {
    this.blockPoolId = blockPoolId;
  }

  @Override
  public ProvidedStorageMap getProvidedStorageMap() {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getProvidedStorageMap!");
  }

  @Override
  public void start(Configuration conf, long blockTotal) {
    mxBeanName = MBeans.register("NameNode", "BlockStats", this);
  }

  @Override
  public void close() {

  }

  @Override
  public void shutdown() {
    MBeans.unregister(mxBeanName);
    mxBeanName = null;
  }

  public Map<Long, ContainerWithPipeline> refreshPipeline(
      Set<Long> containerIDs) throws IOException {
    // TODO: fix Some tests that may not initialize container client
    // The production should always have containerClient initialized.
    if (scmClient.getContainerClient() == null ||
        containerIDs == null || containerIDs.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap = new HashMap<>();

    try {
      List<ContainerWithPipeline> cpList = scmClient.getContainerClient().
          getContainerWithPipelineBatch(new ArrayList<>(containerIDs));
      for (ContainerWithPipeline cp : cpList) {
        containerWithPipelineMap.put(
            cp.getContainerInfo().getContainerID(), cp);
      }
      return containerWithPipelineMap;
    } catch (IOException ioEx) {
      LOG.debug("Get containerPipeline failed for {}",
          containerIDs.toString(), ioEx);
      throw new IOException("SCM_GET_PIPELINE_EXCEPTION", ioEx);
    }
  }

  public List<HDDSLocatedBlock> getHDDSBlockFromSCM(
      ExcludeList excludeList,
      long blockSize,
      int numTargets) throws IOException {
    List<HDDSLocatedBlock> locationInfos = new ArrayList<>(1);
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(blockSize, 1,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.valueOf(numTargets),
              namesystem.getClusterId(),
              excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new IOException("SCM_IN_SAFE_MODE", ex);
      }
      throw ex;
    }
    for (AllocatedBlock allocatedBlock : allocatedBlocks) {
      HDDSLocatedBlock.Builder builder = new HDDSLocatedBlock.Builder()
          .setBlockID(new BlockID(allocatedBlock.getBlockID()))
          .setLength(blockSize)
          .setOffset(0)
          .setPipeline(allocatedBlock.getPipeline());
      // TODO(baoloongmao): add block Token later
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  public void clear() {

  }

  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  @Override
  public void clearQueues() {

  }

  @Override
  public Object setProperty(String property, String newValue)
      throws IllegalArgumentException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support setProperty!");
  }

  @Override
  public Object getProperty(String property) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getProperty!");
  }

  /**
   * Dump meta data to out.
   *
   * @param out
   */
  @Override
  public void metaSave(PrintWriter out) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support metaSave!");
  }

  /**
   * Create SPS manager instance. It manages the user invoked sps paths and does
   * the movement.
   *
   * @param conf    configuration
   * @param spsMode satisfier mode
   * @return true if the instance is successfully created, false otherwise.
   */
  @Override
  public boolean createSPSManager(Configuration conf, String spsMode) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support createSPSManager!");
  }

  /**
   * Nullify SPS manager as this feature is disabled fully.
   */
  @Override
  public void disableSPS() {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support disableSPS!");
  }

  /**
   * @return current access keys.
   */
  @Override
  public ExportedBlockKeys getBlockKeys() {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getBlockKeys!");
  }

  /**
   * Generate a block token for the located block.
   *
   * @param b
   * @param mode
   */
  @Override
  public void setBlockToken(LocatedBlock b,
      BlockTokenIdentifier.AccessMode mode) throws IOException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support setBlockToken!");
  }

  /**
   * Generate a data encryption key for this block pool.
   *
   * @return a data encryption key which may be used to encrypt traffic
   * over the DataTransferProtocol
   */
  @Override
  public DataEncryptionKey generateDataEncryptionKey() {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support generateDataEncryptionKey!");
  }

  @Override
  public boolean shouldUpdateBlockKey(long updateTime) throws IOException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support shouldUpdateBlockKey!");
  }

  @Override
  public int getDefaultStorageNum(BlockInfo block) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getDefaultStorageNum!");
  }

  @Override
  public short getMinReplication() {
    return minReplication;
  }

  @Override
  public short getMinStorageNum(BlockInfo block) {
    return minStorageNum;
  }

  @Override
  public short getMinReplicationToBeInMaintenance() {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getMinReplicationToBeInMaintenance!");
  }

  @Override
  public boolean hasMinStorage(BlockInfo block) {
    return true;
  }

  @Override
  public boolean hasMinStorage(BlockInfo block, int liveNum) {
    return true;
  }

  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum redundancy requirement
   *
   * @param bc          block collection
   * @param commitBlock - contains client reported block length and generation
   * @param iip         - INodes in path to bc
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   *                     of replicas reported from data-nodes.
   */
  @Override
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock, INodesInPath iip) throws IOException {
    if (commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = bc.getLastBlock();
    if (lastBlock == null)
      return false; // no blocks in file yet
    if (lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    if (lastBlock.isUnderRecovery()) {
      throw new IOException("Commit or complete block " + commitBlock +
          ", whereas it is under recovery.");
    }

    final boolean committed = commitBlock(lastBlock, commitBlock);
    if (committed && lastBlock.isStriped()) {
      // update scheduled size for DatanodeStorages that do not store any
      // internal blocks
      lastBlock.getUnderConstructionFeature()
          .updateStorageScheduledSize((BlockInfoStriped) lastBlock);
    }

    // Count replicas on decommissioning nodes, as these will not be
    // decommissioned unless recovery/completing last block has finished
    NumberReplicas numReplicas = countNodes(lastBlock);
    int numUsableReplicas = numReplicas.liveReplicas() +
        numReplicas.decommissioning() +
        numReplicas.liveEnteringMaintenanceReplicas();

    if (hasMinStorage(lastBlock, numUsableReplicas)) {
      if (committed) {
        addExpectedReplicasToPending(lastBlock);
      }
      completeBlock(lastBlock, iip, false);
    }
    return committed;
  }

  private boolean commitBlock(final BlockInfo block,
      final Block commitBlock) throws IOException {
    if (block.getBlockUCState() == HdfsServerConstants.BlockUCState.COMMITTED)
      return false;
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
        "commitBlock length is less than the stored one "
            + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    if (block.getGenerationStamp() != commitBlock.getGenerationStamp()) {
      throw new IOException("Commit block with mismatching GS. NN has " +
          block + ", client submits " + commitBlock);
    }
    List<ReplicaUnderConstruction> staleReplicas =
        block.commitBlock(commitBlock);
    return true;
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @param curBlock - block to be completed
   * @param iip - INodes in path to file containing curBlock; if null,
   *              this will be resolved internally
   * @param force - force completion of the block
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void completeBlock(BlockInfo curBlock, INodesInPath iip,
      boolean force) throws IOException {
    if (curBlock.isComplete()) {
      return;
    }

    int numNodes = curBlock.numNodes();
    if (!force && !hasMinStorage(curBlock, numNodes)) {
      throw new IOException("Cannot complete block: "
          + "block does not satisfy minimal replication requirement.");
    }
    if (!force && curBlock.getBlockUCState() != HdfsServerConstants.BlockUCState.COMMITTED) {
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    }

    convertToCompleteBlock(curBlock, iip);
  }

  /**
   * Convert a specified block of the file to a complete block.
   * Skips validity checking and safe mode block total updates; use
   * @param curBlock - block to be completed
   * @param iip - INodes in path to file containing curBlock; if null,
   *              this will be resolved internally
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void convertToCompleteBlock(BlockInfo curBlock, INodesInPath iip)
      throws IOException {
    curBlock.convertToCompleteBlock();
    namesystem.getFSDirectory().updateSpaceForCompleteBlock(curBlock, iip);
  }

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   *
   * @param block
   */
  @Override
  public void forceCompleteBlock(BlockInfo block) throws IOException {
    List<ReplicaUnderConstruction> staleReplicas = block.commitBlock(block);
    completeBlock(block, null, true);
  }

  /**
   * If IBR is not sent from expected locations yet, add the datanodes to
   * pendingReconstruction in order to keep RedundancyMonitor from scheduling
   * the block.
   *
   * @param blk
   */
  @Override
  public void addExpectedReplicasToPending(BlockInfo blk) {
  }

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
   * @param bc            file
   * @param bytesToRemove num of bytes to remove from block
   * @return the last block locations if the block is partial or null otherwise
   */
  @Override
  public LocatedBlock convertLastBlockToUnderConstruction(BlockCollection bc,
      long bytesToRemove) throws IOException {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support convertLastBlockToUnderConstruction!");
  }

  /**
   * Create a LocatedBlocks.
   *
   * @param blocks
   * @param fileSizeExcludeBlocksUnderConstruction
   * @param isFileUnderConstruction
   * @param offset
   * @param length
   * @param needBlockToken
   * @param inSnapshot
   * @param feInfo
   * @param ecPolicy
   */
  @Override
  public LocatedBlocks createLocatedBlocks(BlockInfo[] blocks,
      long fileSizeExcludeBlocksUnderConstruction,
      boolean isFileUnderConstruction, long offset, long length,
      boolean needBlockToken, boolean inSnapshot, FileEncryptionInfo feInfo,
      ErasureCodingPolicy ecPolicy) throws IOException {
    assert namesystem.hasReadLock();
    if (blocks == null) {
      return null;
    } else if (blocks.length == 0) {
      return new HDDSLocatedBlocks(0L, isFileUnderConstruction,
          Collections.emptyList(), null, false, feInfo, ecPolicy);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = {}", java.util.Arrays.asList(blocks));
      }
      final BlockTokenIdentifier.AccessMode
          mode = needBlockToken? BlockTokenIdentifier.AccessMode.READ: null;

      HddsLocatedBlockBuilder locatedBlocks =
          new HddsLocatedBlockBuilder(Integer.MAX_VALUE)
              .fileLength(fileSizeExcludeBlocksUnderConstruction)
              .lastUC(isFileUnderConstruction)
              .encryption(feInfo)
              .erasureCoding(ecPolicy);

      createLocatedBlockList(locatedBlocks, blocks, offset, length, mode);
      // TODO(baoloongmao): handle last block correctly.
      HDDSLocatedBlocks locations = locatedBlocks.build();
      // Set caching information for the located blocks.
      // TODO(baoloongmao): open cache when we plan to use it.
      return locations;
    }
  }

  @Override
  public void addKeyUpdateCommand(List<DatanodeCommand> cmds,
      DatanodeDescriptor nodeinfo) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support addKeyUpdateCommand!");
  }

  /**
   * Set replication for the blocks.
   *
   * @param oldRepl
   * @param newRepl
   * @param b
   */
  @Override
  public void setReplication(short oldRepl, short newRepl, BlockInfo b) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support setReplication!");
  }

  /**
   * Check whether the specified replication is between the minimum and the
   * maximum replication levels. True return it, otherwise return the minimum
   * or maximum accordingly
   *
   * @param replication
   */
  @Override
  public short adjustReplication(short replication) {
    return replication < minReplication? minReplication
        : replication > maxReplication? maxReplication: replication;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration and throw an exception if it's not.
   *
   * @param src         the path to the target file
   * @param replication the requested replication factor
   * @param clientName  the name of the client node making the request
   * @throws IOException thrown if the requested replication factor
   *                     is out of bounds
   */
  @Override
  public void verifyReplication(String src, short replication,
      String clientName) throws IOException {
  }

  /**
   * Check if a block is replicated to at least the minimum replication.
   *
   * @param b
   */
  @Override
  public boolean isSufficientlyReplicated(BlockInfo b) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support isSufficientlyReplicated!");
  }

  /**
   * Get all blocks with location information from a datanode.
   *
   * @param datanode
   * @param size
   * @param minBlockSize
   */
  @Override
  public BlocksWithLocations getBlocksWithLocations(DatanodeID datanode,
      long size, long minBlockSize) throws UnregisteredNodeException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support getBlocksWithLocations!");
  }

  /**
   * Remove the blocks associated to the given datanode.
   *
   * @param node
   */
  @Override
  public void removeBlocksAssociatedTo(DatanodeDescriptor node) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support removeBlocksAssociatedTo!");
  }

  /**
   * Remove the blocks associated to the given DatanodeStorageInfo.
   *
   * @param storageInfo
   */
  @Override
  public void removeBlocksAssociatedTo(DatanodeStorageInfo storageInfo) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support removeBlocksAssociatedTo!");
  }

  /**
   * Mark the block belonging to datanode as corrupt
   *
   * @param blk       Block to be marked as corrupt
   * @param dn        Datanode which holds the corrupt replica
   * @param storageID if known, null otherwise.
   * @param reason    a textual reason why the block should be marked corrupt,
   */
  @Override
  public void findAndMarkBlockAsCorrupt(ExtendedBlock blk, DatanodeInfo dn,
      String storageID, String reason) throws IOException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support findAndMarkBlockAsCorrupt!");
  }

  /**
   * Choose target for WebHDFS redirection.
   *
   * @param src
   * @param clientnode
   * @param excludes
   * @param blocksize
   */
  @Override
  public DatanodeStorageInfo[] chooseTarget4WebHDFS(String src,
      DatanodeDescriptor clientnode, Set<Node> excludes, long blocksize) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support findAndMarkBlockAsCorrupt!");
  }

  /**
   * Choose target for getting additional datanodes for an existing pipeline.
   *
   * @param src
   * @param numAdditionalNodes
   * @param clientnode
   * @param chosen
   * @param excludesm
   * @param blocksize
   * @param storagePolicyID
   * @param blockType
   */
  @Override
  public DatanodeStorageInfo[] chooseTarget4AdditionalDatanode(String src,
      int numAdditionalNodes, Node clientnode, List<DatanodeStorageInfo> chosen,
      Set<Node> excludesm, long blocksize, byte storagePolicyID,
      BlockType blockType) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support chooseTarget4AdditionalDatanode!");
  }

  /**
   * Choose target datanodes for creating a new block.
   *
   * @param src
   * @param numOfReplicas
   * @param client
   * @param excludedNodes
   * @param blocksize
   * @param favoredNodes
   * @param storagePolicyID
   * @param blockType
   * @param ecPolicy
   * @param flags
   * @throws IOException if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, Node,
   * Set, long, List, BlockStoragePolicy, EnumSet)
   */
  @Override
  public DatanodeStorageInfo[] chooseTarget4NewBlock(String src,
      int numOfReplicas, Node client, Set<Node> excludedNodes, long blocksize,
      List<String> favoredNodes, byte storagePolicyID, BlockType blockType,
      ErasureCodingPolicy ecPolicy, EnumSet<AddBlockFlag> flags)
      throws IOException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support chooseTarget4NewBlock!");
  }

  @Override
  public long requestBlockReportLeaseId(DatanodeRegistration nodeReg) {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support requestBlockReportLeaseId!");
  }

  @Override
  public void registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    throw new UnsupportedOperationException(
        "HddsBlockManager does not support registerDatanode!");
  }

  /**
   * Set the total number of blocks in the system.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param total
   */
  @Override
  public void setBlockTotal(long total) {
  }

  /**
   * Removes the blocks from blocksmap and updates the safemode blocks total.
   *
   * @param blocks An instance of {@link INode.BlocksMapUpdateInfo} which
   *              contains a list of blocks that need to be removed from
   *              blocksMap
   */
  @Override
  public void removeBlocksAndUpdateSafemodeTotal(
      INode.BlocksMapUpdateInfo blocks) {
    assert namesystem.hasWriteLock();
  }

  /**
   * Check block report lease.
   *
   * @param context
   * @param nodeID
   * @return true if lease exist and not expire
   */
  @Override
  public boolean checkBlockReportLease(BlockReportContext context,
      DatanodeID nodeID) throws UnregisteredNodeException {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support checkBlockReportLease!");
  }

  /**
   * The given storage is reporting all its blocks.
   * Update the (storage-->block list) and (block-->storage list) maps.
   *
   * @param nodeID
   * @param storage
   * @param newReport
   * @param context
   * @return true if all known storages of the given DN have finished reporting.
   * @throws IOException
   */
  @Override
  public boolean processReport(DatanodeID nodeID, DatanodeStorage storage,
      BlockListAsLongs newReport, BlockReportContext context)
      throws IOException {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support processReport!");
  }

  /**
   * processFirstBlockReport is intended only for processing "initial" block
   * reports, the first block report received from a DN after it registers.
   * It just adds all the valid replicas to the datanode, without calculating
   * a toRemove list (since there won't be any).  It also silently discards
   * any invalid blocks, thereby deferring their processing until
   * the next block report.
   *
   * @param storageInfo - DatanodeStorageInfo that sent the report
   * @param report      - the initial block report, to be processed
   * @throws IOException
   */
  @Override
  public void processFirstBlockReport(DatanodeStorageInfo storageInfo,
      BlockListAsLongs report) throws IOException {
  }

  /**
   * The given node is reporting incremental information about some blocks.
   * This includes blocks that are starting to be received, completed being
   * received, or deleted.
   * <p>
   * This method must be called with FSNamesystem lock held.
   *
   * @param nodeID
   * @param srdb
   */
  @Override
  public void processIncrementalBlockReport(DatanodeID nodeID,
      StorageReceivedDeletedBlocks srdb) throws IOException {
  }

  @Override
  public void removeBRLeaseIfNeeded(DatanodeID nodeID,
      BlockReportContext context) throws IOException {
  }

  /**
   * Mark block replicas as corrupt except those on the storages in
   * newStorages list.
   *
   * @param oldBlock
   * @param block
   * @param oldGenerationStamp
   * @param oldNumBytes
   * @param newStorages
   */
  @Override
  public void markBlockReplicasAsCorrupt(Block oldBlock, BlockInfo block,
      long oldGenerationStamp, long oldNumBytes,
      DatanodeStorageInfo[] newStorages) throws IOException {
  }

  /**
   * Try to process any messages that were previously queued for the given
   * block. This is called from FSEditLogLoader whenever a block's state
   * in the namespace has changed or a new block has been created.
   *
   * @param b
   */
  @Override
  public void processQueuedMessagesForBlock(Block b) throws IOException {
  }

  /**
   * Process any remaining queued datanode messages after entering
   * active state. At this point they will not be re-queued since
   * we are the definitive master node and thus should be up-to-date
   * with the namespace information.
   */
  @Override
  public void processAllPendingDNMessages() throws IOException {
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * extra or low redundancy. Place it into the respective queue.
   */
  @Override
  public void processMisReplicatedBlocks() {
  }

  /**
   * Get the value of whether there are any non-EC blocks using StripedID.
   *
   * @return Returns the value of whether there are any non-EC blocks using
   * StripedID.
   */
  @Override
  public boolean hasNonEcBlockUsingStripedID() {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support hasNonEcBlockUsingStripedID!");
  }

  @Override
  public void setPostponeBlocksFromFuture(boolean postpone) {
  }

  /**
   * The given node is reporting that it received a certain block.
   *
   * @param storageInfo
   * @param block
   * @param delHint
   */
  @Override
  public void addBlock(DatanodeStorageInfo storageInfo, Block block,
      String delHint) throws IOException {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support addBlock!");
  }

  @Override
  public void removeBlock(BlockInfo block) {
    assert namesystem.hasWriteLock();
    // No need to ACK blocks that are being removed entirely
    // from the namespace, since the removal of the associated
    // file already removes them from the block map below.
    block.setNumBytes(BlockCommand.NO_ACK);
    removeBlockFromMap(block);
  }

  /**
   * Return the number of nodes hosting a given block, grouped
   * by the state of those replicas.
   * For a striped block, this includes nodes storing blocks belonging to the
   * striped block group. But note we exclude duplicated internal block replicas
   * for calculating {@link NumberReplicas#liveReplicas}.
   *
   * @param b
   */
  @Override
  public NumberReplicas countNodes(BlockInfo b) {
    NumberReplicas numberReplicas = new NumberReplicas();
    return numberReplicas;
  }

  @Override
  public boolean isExcess(DatanodeDescriptor dn, BlockInfo blk) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support isExcess!");
  }

  @Override
  public DatanodeStorageInfo[] getStorages(BlockInfo block) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getStorages!");
  }

  /**
   * @param block
   * @return an iterator of the datanodes.
   */
  @Override
  public Iterable<DatanodeStorageInfo> getStorages(Block block) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getStorages!");
  }

  /**
   * Get the block info of a Block.
   *
   * @param block
   */
  @Override
  public BlockInfo getStoredBlock(Block block) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getStoredBlock!");
  }

  @Override
  public void updateLastBlock(BlockInfo lastBlock, ExtendedBlock newBlock) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support updateLastBlock!");
  }

  /**
   * Check sufficient redundancy of the blocks in the collection. If any block
   * is needed reconstruction, insert it into the reconstruction queue.
   * Otherwise, if the block is more than the expected replication factor,
   * process it as an extra redundancy block.
   *
   * @param bc
   */
  @Override
  public void checkRedundancy(BlockCollection bc) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support checkRedundancy!");
  }

  @Override
  public boolean isPlacementPolicySatisfied(BlockInfo storedBlock) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support isPlacementPolicySatisfied!");
  }

  @Override
  public boolean isNeededReconstructionForMaintenance(BlockInfo storedBlock,
      NumberReplicas numberReplicas) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support isNeededReconstructionForMaintenance!");
  }

  /**
   * A block needs reconstruction if the number of redundancies is less than
   * expected or if it does not have enough racks.
   *
   * @param storedBlock
   * @param numberReplicas
   */
  @Override
  public boolean isNeededReconstruction(BlockInfo storedBlock,
      NumberReplicas numberReplicas) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support isNeededReconstruction!");
  }

  @Override
  public short getExpectedLiveRedundancyNum(BlockInfo block,
      NumberReplicas numberReplicas) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getExpectedLiveRedundancyNum!");
  }

  @Override
  public BlockInfo addBlockCollection(BlockInfo block, BlockCollection bc) {
    block.setBlockCollectionId(bc.getId());
    blockNum.incrementAndGet();
    return block;
  }

  /**
   * Do some check when adding a block to blocksmap.
   * For HDFS-7994 to check whether then block is a NonEcBlockUsingStripedID.
   *
   * @param block
   * @param bc
   */
  @Override
  public BlockInfo addBlockCollectionWithCheck(BlockInfo block,
      BlockCollection bc) {
    block.setBlockCollectionId(bc.getId());
    blockNum.incrementAndGet();
    return block;
  }

  /**
   * Get number of corrupt replicas of a given block.
   *
   * @param block
   */
  @Override
  public int numCorruptReplicas(Block block) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support numCorruptReplicas!");
  }

  /**
   * Free the block from Block Manager management.
   *
   * @param block
   */
  @Override
  public void removeBlockFromMap(BlockInfo block) {
    assert block instanceof HddsBlockInfo;
    blockNum.decrementAndGet();
    // TODO(baoloongmao): micahzhao will create a member invalidateBlocks to
    //  make remove async.
    List<BlockGroup> toDeleteBlockGroupList = new ChunkedArrayList<>();
    List<BlockID> item = new ArrayList<>();
    item.add(new BlockID(((HddsBlockInfo) block).getContainerID(),
        block.getBlockId()));
    BlockGroup keyBlocks = BlockGroup.newBuilder()
        .setKeyName(block.getBlockName()).addAllBlockIDs(item).build();
    toDeleteBlockGroupList.add(keyBlocks);
    try {
      if (toDeleteBlockGroupList != null && !toDeleteBlockGroupList.isEmpty()) {
        scmClient.getBlockClient().deleteKeyBlocks(toDeleteBlockGroupList);
      }
    } catch (IOException e) {
      LOG.error("Error while deleting key blocks.", e);
    }
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  @Override
  public Iterator<BlockInfo> getCorruptReplicaBlockIterator() {
    return new Iterator<BlockInfo>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public BlockInfo next() {
        return null;
      }
    };
  }

  /**
   * Get the locations of replicas which are corrupt for a given block.
   *
   * @param block
   */
  @Override
  public Collection<DatanodeDescriptor> getCorruptReplicas(Block block) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getCorruptReplicas!");
  }

  /**
   * Get reason for certain corrupted replicas for a given block and a given dn.
   *
   * @param block
   * @param node
   */
  @Override
  public String getCorruptReason(Block block, DatanodeDescriptor node) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getCorruptReason!");
  }

  /**
   * Initialize replication queues.
   */
  @Override
  public void initializeReplQueues() {

  }

  @Override
  public void setInitializedReplQueues(boolean v) {

  }

  /**
   * Check if replication queues are to be populated.
   *
   * @return true when node is HAState.Active and not in the very first safemode
   */
  @Override
  public boolean isPopulatingReplQueues() {
    return false;
  }

  /**
   * Check if start to add replication command to replication manager
   */
  @Override
  public boolean shouldPopulateReplQueues() {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support shouldPopulateReplQueues!");
  }

  @Override
  public boolean getShouldPostponeBlocksFromFuture() {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getShouldPostponeBlocksFromFuture!");
  }

  @Override
  public void enqueueBlockOp(Runnable action) throws IOException {
    throw new UnsupportedOperationException("HDDSBlockManager does not " +
        "support enqueueBlockOp!");
  }

  @Override
  public <T> T runBlockOp(Callable<T> action) throws IOException {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support runBlockOp!");
  }

  /**
   * Notification of a successful block recovery.
   *
   * @param block for which the recovery succeeded
   */
  @Override
  public void successfulBlockRecovery(BlockInfo block) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support successfulBlockRecovery!");
  }

  /**
   * Checks whether a recovery attempt has been made for the given block.
   * If so, checks whether that attempt has timed out.
   *
   * @param b block for which recovery is being attempted
   * @return true if no recovery attempt has been made or
   * the previous attempt timed out
   */
  @Override
  public boolean addBlockRecoveryAttempt(BlockInfo b) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support addBlockRecoveryAttempt!");
  }

  @Override
  public void flushBlockOps() throws IOException {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support flushBlockOps!");
  }

  @Override
  public BlockPlacementPolicy getBlockPlacementPolicy() {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getBlockPlacementPolicy!");
  }

  @Override
  public void setBlockRecoveryTimeout(long blockRecoveryTimeout) {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support setBlockRecoveryTimeout!");
  }

  /**
   * The statistics of storage types.
   *
   * @return get storage statistics per storage type
   */
  @Override
  public Map<StorageType, StorageTypeStats> getStorageTypeStats() {
    throw new UnsupportedOperationException("HddsBlockManager does not " +
        "support getStorageTypeStats!");
  }

  /**
   * Gets the total numbers of blocks on the cluster.
   *
   * @return the total number of blocks of the cluster
   */
  @Override
  public long getTotalBlocks() {
    return blockNum.get();
  }

  @Override
  public int getActiveBlockCount() {
    return 0;
  }

  /**
   * @return the size of UnderReplicatedBlocks
   */
  @Override
  public int numOfUnderReplicatedBlocks() {
    return 0;
  }

  @Override
  public long getHighestPriorityReplicatedBlockCount() {
    return 0;
  }

  @Override
  public long getHighestPriorityECBlockCount() {
    return 0;
  }

  @Override
  public int getCapacity() {
    return 1024 * 1024 * 1024;
  }

  @Override
  public long getProvidedCapacity() {
    return 0;
  }

  /**
   * Get aggregated count of all blocks with low redundancy.
   */
  @Override
  public long getLowRedundancyBlocksCount() {
    return 0;
  }

  /**
   * Returns number of blocks with corrupt replicas
   */
  @Override
  public long getCorruptReplicaBlocksCount() {
    return 0;
  }

  @Override
  public long getScheduledReplicationBlocksCount() {
    return 0;
  }

  @Override
  public long getPendingDeletionBlocksCount() {
    return 0;
  }

  @Override
  public long getMissingBlocksCount() {
    return 0;
  }

  @Override
  public long getExcessBlocksCount() {
    return 0;
  }

  @Override
  public long getMissingReplOneBlocksCount() {
    return 0;
  }

  @Override
  public long getNumTimedOutPendingReconstructions() {
    return 0;
  }

  /**
   * Get aggregated count of all blocks pending to be reconstructed.
   */
  @Override
  public long getPendingReconstructionBlocksCount() {
    return 0;
  }

  @Override
  public long getStartupDelayBlockDeletionInMs() {
    return 0;
  }

  @Override
  public int getUnderReplicatedNotMissingBlocks() {
    return 0;
  }

  @Override
  public long getPostponedMisreplicatedBlocksCount() {
    return 0;
  }

  @Override
  public int getPendingDataNodeMessageCount() {
    return 0;
  }

  @Override
  public BlockStoragePolicySuite getStoragePolicySuite() {
    return storagePolicySuite;
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String policyName) {
    return storagePolicySuite.getPolicy(policyName);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(byte policyId) {
    return storagePolicySuite.getPolicy(policyId);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() {
    throw new UnsupportedOperationException("HDDSBlockManager does not " +
        "support getStoragePolicies!");
  }

  /**
   * Return count of erasure coded block groups with low redundancy.
   */
  @Override
  public long getLowRedundancyECBlockGroups() {
    return 0;
  }

  /**
   * Return count of erasure coded block groups that are corrupt.
   */
  @Override
  public long getCorruptECBlockGroups() {
    return 0;
  }

  /**
   * Return count of erasure coded block groups that are missing.
   */
  @Override
  public long getMissingECBlockGroups() {
    return 0;
  }

  /**
   * Return total bytes of erasure coded future block groups.
   */
  @Override
  public long getBytesInFutureECBlockGroups() {
    return 0;
  }

  /**
   * Return count of erasure coded blocks that are pending deletion.
   */
  @Override
  public long getPendingDeletionECBlocks() {
    return 0;
  }

  /**
   * Return total number of erasure coded block groups.
   */
  @Override
  public long getTotalECBlockGroups() {
    return 0;
  }

  /**
   * @return the enabled erasure coding policies separated with comma.
   */
  @Override
  public String getEnabledEcPolicies() {
    return null;
  }

  /**
   * Return low redundancy blocks count.
   */
  @Override
  public long getLowRedundancyReplicatedBlocks() {
    return 0;
  }

  /**
   * Return corrupt blocks count.
   */
  @Override
  public long getCorruptReplicatedBlocks() {
    return 0;
  }

  /**
   * Return missing blocks count.
   */
  @Override
  public long getMissingReplicatedBlocks() {
    return 0;
  }

  /**
   * Return count of missing blocks with replication factor one.
   */
  @Override
  public long getMissingReplicationOneBlocks() {
    return 0;
  }

  /**
   * Return total bytes of future blocks.
   */
  @Override
  public long getBytesInFutureReplicatedBlocks() {
    return 0;
  }

  /**
   * Return count of blocks that are pending deletion.
   */
  @Override
  public long getPendingDeletionReplicatedBlocks() {
    return 0;
  }

  /**
   * Return total number of replicated blocks.
   */
  @Override
  public long getTotalReplicatedBlocks() {
    return blockNum.get();
  }

  /**
   * Return high priority replicated blocks count.
   */
  @Override
  public long getHighestPriorityLowRedundancyReplicatedBlocks() {
    return 0;
  }
}

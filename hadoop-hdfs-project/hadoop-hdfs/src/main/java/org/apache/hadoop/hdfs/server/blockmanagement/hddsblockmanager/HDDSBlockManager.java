package org.apache.hadoop.hdfs.server.blockmanagement.hddsblockmanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.HDDSLocatedBlocks;
import org.apache.hadoop.hdds.HDDSLocationInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.ScmClient;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.HDDSServerLocationInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HDDSBlockManager {
  public static final Logger LOG = LoggerFactory.getLogger(HDDSBlockManager.class);
  private final ScmClient scmClient;
  private final FSNamesystem namesystem;

  public HDDSBlockManager(final FSNamesystem namesystem, boolean haEnabled,
      final Configuration conf) throws IOException {
    OzoneConfiguration o3Conf = OzoneConfiguration.of(conf);
    ScmBlockLocationProtocol scmBlockClient =
        ScmClient.getScmBlockClient(o3Conf);
    StorageContainerLocationProtocol scmContainerClient =
        ScmClient.getScmContainerClient(o3Conf);
    this.namesystem = namesystem;
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient);
  }

  public String getBlockPoolId() {
    try {
      return scmClient != null && scmClient.getContainerClient() != null ?
          scmClient.getContainerClient().getScmInfo().getScmId() :
          "N/A";
    } catch (IOException e) {
      return "N/A";
    }
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

  public List<HDDSLocationInfo> getHDDSBlockFromSCM(
      ExcludeList excludeList,
      long blockSize,
      int numTargets) throws IOException {
    List<HDDSLocationInfo> locationInfos = new ArrayList<>(1);
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
      HDDSLocationInfo.Builder builder = new HDDSLocationInfo.Builder()
          .setBlockID(new BlockID(allocatedBlock.getBlockID()))
          .setLength(blockSize)
          .setOffset(0)
          .setPipeline(allocatedBlock.getPipeline());
      // TODO(baoloongmao): add block Token later
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  /**
   * From the given list, remove the blocks use Ozone SCM
   *
   * @param blocksList
   *          An instance of {@link List< BlockGroup >} which contains a list
   *          of blocks that need to be removed
   */
  public void removeBlocks(List<BlockGroup> blocksList) {
    try {
      if (blocksList != null && !blocksList.isEmpty()) {
        scmClient.getBlockClient().deleteKeyBlocks(blocksList);
      }
    } catch (IOException e) {
      LOG.error("Error while deleting key blocks.", e);
    }
  }

  public void clear() {
  }

  public HDDSLocatedBlocks createLocatedBlocks(
      final HDDSServerLocationInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken,
      final boolean inSnapshot, FileEncryptionInfo feInfo,
      ErasureCodingPolicy ecPolicy)
      throws IOException {
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

      HDDSLocatedBlockBuilder locatedBlocks =
          new HDDSLocatedBlockBuilder(Integer.MAX_VALUE)
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

  private void createLocatedBlockList(
      HDDSLocatedBlockBuilder locatedBlocks,
      final HDDSServerLocationInfo[] blocks,
      final long offset, final long length,
      final BlockTokenIdentifier.AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getLength() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getLength();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return;

    long endOff = offset + length;
    do {
      locatedBlocks.addBlock(
          createLocatedBlock(locatedBlocks, blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getLength();
      curBlk++;
    } while (curPos < endOff
        && curBlk < blocks.length
        && !locatedBlocks.isBlockMax());
    return;
  }

  private HDDSLocationInfo createLocatedBlock(HDDSLocatedBlockBuilder locatedBlocks,
      final HDDSServerLocationInfo blk, final long pos, final BlockTokenIdentifier.AccessMode mode)
      throws IOException {
    // TODO(baoloongmao): open block token until necessary
    HDDSLocationInfo locationInfo = null;
    if (blk != null) {
      locationInfo = new HDDSLocationInfo.Builder()
          .setBlockID(blk.getBlockID())
          .setPipeline(blk.getPipeline())
          .setLength(blk.getLength())
          .setOffset(blk.getOffset())
          .setToken(blk.getToken())
          .build();
    }
    return locationInfo;
  }
}

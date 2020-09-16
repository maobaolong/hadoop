package org.apache.hadoop.hdfs.server.blockmanagement.hddsblockmanager;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
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
  private final BlockStoragePolicySuite storagePolicySuite;

  public HDDSBlockManager(final FSNamesystem namesystem, boolean haEnabled,
      final Configuration conf) throws IOException {
    OzoneConfiguration o3Conf = OzoneConfiguration.of(conf);
    ScmBlockLocationProtocol scmBlockClient =
        ScmClient.getScmBlockClient(o3Conf);
    StorageContainerLocationProtocol scmContainerClient =
        ScmClient.getScmContainerClient(o3Conf);
    this.namesystem = namesystem;
    this.scmClient = new ScmClient(scmBlockClient, scmContainerClient);
    storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite();
  }

  public ScmClient getScmClient() {
    return scmClient;
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
}

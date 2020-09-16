/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.hddsdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.HDDSLocationInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager.
 */
public class BlockOutputStreamEntryPool {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStreamEntryPool.class);

  private final List<BlockOutputStreamEntry> streamEntries;
  private final HdfsFileStatus stat;
  private int currentStreamIndex;
  private final DFSClient dfsClient;
  private final XceiverClientManager xceiverClientManager;
  private final int chunkSize;
  private final String requestID;
  private final int streamBufferSize;
  private final long streamBufferFlushSize;
  private final boolean streamBufferFlushDelay;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private final long blockSize;
  private final int bytesPerChecksum;
  private final ContainerProtos.ChecksumType checksumType;
  private final BufferPool bufferPool;
  private final long openID;
  private final ExcludeList excludeList;

  private final String src;
  private HDDSLocationInfo currentBlock;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public BlockOutputStreamEntryPool(DFSClient dfsClient,
      String src,
      int chunkSize, String requestId, HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType type,
      int bufferSize, long bufferFlushSize,
      boolean bufferFlushDelay, long bufferMaxSize,
      long size, long watchTimeout, ContainerProtos.ChecksumType checksumType,
      int bytesPerChecksum,
      XceiverClientManager xceiverClientManager, long openID,
      HdfsFileStatus stat) {
    streamEntries = new ArrayList<>();
    currentStreamIndex = 0;
    this.dfsClient = dfsClient;
    this.xceiverClientManager = xceiverClientManager;
    this.chunkSize = chunkSize;
    this.requestID = requestId;
    this.streamBufferSize = bufferSize;
    this.streamBufferFlushSize = bufferFlushSize;
    this.streamBufferFlushDelay = bufferFlushDelay;
    this.streamBufferMaxSize = bufferMaxSize;
    this.blockSize = size;
    this.watchTimeout = watchTimeout;
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumType = checksumType;
    this.openID = openID;
    this.excludeList = new ExcludeList();
    this.stat = stat;

    Preconditions.checkState(chunkSize > 0);
    Preconditions.checkState(streamBufferSize > 0);
    Preconditions.checkState(streamBufferFlushSize > 0);
    Preconditions.checkState(streamBufferMaxSize > 0);
    Preconditions.checkState(blockSize > 0);
    Preconditions.checkState(blockSize >= streamBufferMaxSize);
    Preconditions.checkState(streamBufferMaxSize % streamBufferFlushSize == 0,
        "expected max. buffer size (%s) to be a multiple of flush size (%s)",
        streamBufferMaxSize, streamBufferFlushSize);
    Preconditions.checkState(streamBufferFlushSize % streamBufferSize == 0,
        "expected flush size (%s) to be a multiple of buffer size (%s)",
        streamBufferFlushSize, streamBufferSize);
    Preconditions.checkState(chunkSize % streamBufferSize == 0,
        "expected chunk size (%s) to be a multiple of buffer size (%s)",
        chunkSize, streamBufferSize);
    this.bufferPool =
        new BufferPool(streamBufferSize,
            (int) (streamBufferMaxSize / streamBufferSize),
            xceiverClientManager.byteBufferToByteStringConversion());

    this.src = src;
  }

  /**
   * A constructor for testing purpose only.
   */
  @VisibleForTesting
  BlockOutputStreamEntryPool() {
    streamEntries = new ArrayList<>();
    dfsClient = null;
    xceiverClientManager = null;
    chunkSize = 0;
    requestID = null;
    streamBufferSize = 0;
    streamBufferFlushSize = 0;
    streamBufferFlushDelay = false;
    streamBufferMaxSize = 0;
    bufferPool = new BufferPool(chunkSize, 1);
    watchTimeout = 0;
    blockSize = 0;
    this.checksumType = ContainerProtos.ChecksumType.valueOf(
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT);
    this.bytesPerChecksum = OzoneConfigKeys
        .OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES; // Default is 1MB
    currentStreamIndex = 0;
    openID = -1;
    excludeList = new ExcludeList();

    src = "";
    stat = null;
  }

  private void addKeyLocationInfo(HDDSLocationInfo hddsLocationInfo)
      throws IOException {
    Preconditions.checkNotNull(hddsLocationInfo.getPipeline());
    UserGroupInformation.getCurrentUser().addToken(hddsLocationInfo.getToken());
    BlockOutputStreamEntry.Builder builder =
        new BlockOutputStreamEntry.Builder()
            .setBlockID(hddsLocationInfo.getBlockID())
            .setKey(src)
            .setXceiverClientManager(xceiverClientManager)
            .setPipeline(hddsLocationInfo.getPipeline())
            .setRequestId(requestID)
            .setChunkSize(chunkSize)
            .setLength(hddsLocationInfo.getLength())
            .setStreamBufferSize(streamBufferSize)
            .setStreamBufferFlushSize(streamBufferFlushSize)
            .setStreamBufferFlushDelay(streamBufferFlushDelay)
            .setStreamBufferMaxSize(streamBufferMaxSize)
            .setWatchTimeout(watchTimeout)
            .setbufferPool(bufferPool)
            .setChecksumType(checksumType)
            .setBytesPerChecksum(bytesPerChecksum)
            .setToken(hddsLocationInfo.getToken());
    streamEntries.add(builder.build());
    this.currentBlock = hddsLocationInfo;
  }

  public List<HDDSLocationInfo> getLocationInfoList()  {
    List<HDDSLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockOutputStreamEntry streamEntry : streamEntries) {
      long length = streamEntry.getCurrentPosition();

      // Commit only those blocks to OzoneManager which are not empty
      if (length != 0) {
        HDDSLocationInfo info =
            new HDDSLocationInfo.Builder().setBlockID(streamEntry.getBlockID())
                .setLength(streamEntry.getCurrentPosition()).setOffset(0)
                .setToken(streamEntry.getToken())
                .setPipeline(streamEntry.getPipeline()).build();
        locationInfoList.add(info);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "block written " + streamEntry.getBlockID() + ", length " + length
                + " bcsID " + streamEntry.getBlockID()
                .getBlockCommitSequenceId());
      }
    }
    return locationInfoList;
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   * @param pipelineId id of the associated pipeline
   */
  void discardPreallocatedBlocks(long containerID, PipelineID pipelineId) {
    // currentStreamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.

    // This will be called only to discard the next subsequent unused blocks
    // in the streamEntryList.
    if (currentStreamIndex + 1 < streamEntries.size()) {
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex + 1);
      while (streamEntryIterator.hasNext()) {
        BlockOutputStreamEntry streamEntry = streamEntryIterator.next();
        Preconditions.checkArgument(streamEntry.getCurrentPosition() == 0);
        if ((streamEntry.getPipeline().getId().equals(pipelineId)) ||
            (containerID != -1 &&
                streamEntry.getBlockID().getContainerID() == containerID)) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  List<BlockOutputStreamEntry> getStreamEntries() {
    return streamEntries;
  }

  XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  String getKeyName() {
    return "tmpKey";
  }

  long getKeyLength() {
    return streamEntries.stream().mapToLong(
        BlockOutputStreamEntry::getCurrentPosition).sum();
  }
  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new BlockOutputStreamEntry to write.
   *
   * @throws IOException
   */
  private void allocateNewBlock() throws IOException {
    if (!excludeList.isEmpty()) {
      LOG.info("Allocating block with {}", excludeList);
    }
    // update the currentBlock length
    if (currentBlock != null && streamEntries.size() > 0) {
      long lastBlockLength =
          streamEntries.get(streamEntries.size() - 1).getCurrentPosition();
      currentBlock.setLength(lastBlockLength);
    }
    HDDSLocationInfo hddsLocationInfo =
        dfsClient.allocateBlock(src, dfsClient.getClientName(), currentBlock,
            excludeList, stat.getFileId(), openID);
    addKeyLocationInfo(hddsLocationInfo);
  }

  void commitKey(long offset) throws IOException {
    if (src != null) {
      if (streamEntries.size() > 0) {
        long lastBlockLength =
            streamEntries.get(streamEntries.size() - 1).getCurrentPosition();
        currentBlock.setLength(lastBlockLength);
      }
      dfsClient.getNamenode().completeHDDSFile(src,
          dfsClient.getClientName(), currentBlock, stat.getFileId());
    } else {
      LOG.warn("Closing KeyOutputStream, but key args is null");
    }
  }

  public BlockOutputStreamEntry getCurrentStreamEntry() {
    if (streamEntries.isEmpty() || streamEntries.size() <= currentStreamIndex) {
      return null;
    } else {
      return streamEntries.get(currentStreamIndex);
    }
  }

  BlockOutputStreamEntry allocateBlockIfNeeded() throws IOException {
    BlockOutputStreamEntry streamEntry = getCurrentStreamEntry();
    if (streamEntry != null && streamEntry.isClosed()) {
      // a stream entry gets closed either by :
      // a. If the stream gets full
      // b. it has encountered an exception
      currentStreamIndex++;
    }
    if (streamEntries.size() <= currentStreamIndex) {
      Preconditions.checkNotNull(dfsClient);
      // allocate a new block, if a exception happens, log an error and
      // throw exception to the caller directly, and the write fails.
      allocateNewBlock();
    }
    // in theory, this condition should never violate due the check above
    // still do a sanity check.
    Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
    return streamEntries.get(currentStreamIndex);
  }

  long computeBufferData() {
    return bufferPool.computeBufferData();
  }

  void cleanup() {
    if (excludeList != null) {
      excludeList.clear();
    }
    if (bufferPool != null) {
      bufferPool.clearBufferPool();
    }

    if (streamEntries != null) {
      streamEntries.clear();
    }
  }

  public ExcludeList getExcludeList() {
    return excludeList;
  }

  public long getStreamBufferMaxSize() {
    return streamBufferMaxSize;
  }

  boolean isEmpty() {
    return streamEntries.isEmpty();
  }
}

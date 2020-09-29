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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.ClientNamenodeSCMProtocolProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.UnknownPipelineStateException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class HddsBlockInfo extends BlockInfoContiguous
    implements Writable{
  private BlockID blockID;
  // the id of this subkey in all the subkeys.
  private long length;
  private long offset;
  // Block token, required for client authentication when security is enabled.
  // TODO(baoloongmao) take token back when we need.
//  private Token<OzoneBlockTokenIdentifier> token;
  // the version number indicating when this block was added

  public HddsBlockInfo() {
    super((short) 0);
  }

  private HddsBlockInfo(BlockID blockID, long length, long offset) {
    super((short) 0);
    this.blockID = blockID;
    this.length = length;
    this.offset = offset;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public long getContainerID() {
    return blockID.getContainerID();
  }

  public long getLocalID() {
    return blockID.getLocalID();
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public long getBlockCommitSequenceId() {
    return blockID.getBlockCommitSequenceId();
  }

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder {
    private BlockID blockID;
    private long length;
    private long offset;

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

    public Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public Builder setOffset(long off) {
      this.offset = off;
      return this;
    }

    public HddsBlockInfo build() {
      return new HddsBlockInfo(blockID, length, offset);
    }
  }

  public HdfsProtos.HDDSServerLocationInfoProto getProtobuf() {
    return HdfsProtos.HDDSServerLocationInfoProto
        .newBuilder()
        .setContainerId(blockID.getContainerID())
        .setLocalId(blockID.getLocalID())
        .setLength(getLength())
        .setOffset(getOffset())
        .build();
  }

  public static HddsBlockInfo getFromProtobuf(
      HdfsProtos.HDDSServerLocationInfoProto locationInfoProto) {
    return new HddsBlockInfo.Builder()
        .setBlockID(new BlockID(locationInfoProto.getContainerId(),
            locationInfoProto.getLocalId()))
        .setLength(locationInfoProto.getLength())
        .setOffset(locationInfoProto.getOffset())
        .build();
  }

  private static Pipeline getPipeline(
      ClientNamenodeSCMProtocolProtos.HddsLocation keyLocation) {
    try {
      return keyLocation.hasPipeline() ?
          Pipeline.getFromProtobuf(keyLocation.getPipeline()) : null;
    } catch (UnknownPipelineStateException e) {
      return null;
    }
  }

  @Override
  public String toString() {
    return "{blockID={containerID=" + blockID.getContainerID() +
        ", localID=" + blockID.getLocalID() + "}" +
        ", length=" + length +
        ", offset=" + offset +
        '}';
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  @Override // Writable
  public void write(DataOutput out) throws IOException {
    writeHelper(out);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    readHelper(in);
  }

  final void writeHelper(DataOutput out) throws IOException {
    out.writeLong(blockID.getContainerID());
    out.writeLong(blockID.getLocalID());
    out.writeLong(length);
    out.writeLong(offset);
  }

  final void readHelper(DataInput in) throws IOException {
    long containerID = in.readLong();
    long localID = in.readLong();
    this.blockID = new BlockID(containerID, localID);
    this.length = in.readLong();
    this.offset = in.readLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HddsBlockInfo that = (HddsBlockInfo) o;
    return length == that.length &&
        offset == that.offset &&
        Objects.equals(blockID, that.blockID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockID, length, offset);
  }

  public long getNumBytes() {
    return length;
  }
}

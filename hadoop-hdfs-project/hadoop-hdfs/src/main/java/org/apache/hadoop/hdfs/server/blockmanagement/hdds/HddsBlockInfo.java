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

import org.apache.hadoop.hdds.protocol.proto.ClientNamenodeSCMProtocolProtos;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
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
  public static final DatanodeStorageInfo[] DN_STORAGE_INFOS_EMPTY_ARRAY = {};

  // Block token, required for client authentication when security is enabled.
  // TODO(baoloongmao) take token back when we need.
//  private Token<OzoneBlockTokenIdentifier> token;
  // the version number indicating when this block was added
  private long containerId;

  public HddsBlockInfo() {
    super((short) 0);
  }

  private HddsBlockInfo(long containerId, long localID,
      long length) {
    super((short) 0);
    storages = DN_STORAGE_INFOS_EMPTY_ARRAY;
    setBlockId(localID);
    setNumBytes(length);
    this.containerId = containerId;
  }

  public long getContainerID() {
    return this.containerId;
  }

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder {
    private long length;
    private long containerId;
    private long localId;

    public Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public Builder setContainerId(long containerId) {
      this.containerId = containerId;
      return this;
    }

    public Builder setLocalId(long localId) {
      this.localId = localId;
      return this;
    }

    public HddsBlockInfo build() {
      return new HddsBlockInfo(containerId, localId, length);
    }
  }

  public ClientNamenodeSCMProtocolProtos.HddsBlockInfoProto getProtobuf() {
    return ClientNamenodeSCMProtocolProtos.HddsBlockInfoProto
        .newBuilder()
        .setContainerId(getContainerID())
        .setLocalId(getBlockId())
        .setLength(getNumBytes())
        .build();
  }

  public static HddsBlockInfo getFromProtobuf(
      ClientNamenodeSCMProtocolProtos.HddsBlockInfoProto locationInfoProto) {
    return new HddsBlockInfo.Builder()
        .setContainerId(locationInfoProto.getContainerId())
        .setLocalId(locationInfoProto.getLocalId())
        .setLength(locationInfoProto.getLength())
        .build();
  }

  @Override
  public String toString() {
    return "{blockID={containerID=" + getContainerID() +
        ", localID=" + getBlockId() + "}" +
        ", numOfBytes=" + getNumBytes() +
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
    out.writeLong(getContainerID());
    out.writeLong(getBlockId());
    out.writeLong(getNumBytes());
  }

  final void readHelper(DataInput in) throws IOException {
    long containerId = in.readLong();
    long localID = in.readLong();
    this.containerId = containerId;
    setBlockId(localID);
    setNumBytes(in.readLong());
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
    return getNumBytes() == that.getNumBytes() &&
        getContainerID() == that.getContainerID() &&
        getBlockId() == that.getBlockId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getBlockId(), getNumBytes(), getContainerID());
  }

  public String getBlockName() {
    return new StringBuilder().append(BLOCK_FILE_PREFIX)
        .append(getContainerID())
        .append("_")
        .append(getBlockId()).toString();
  }
}

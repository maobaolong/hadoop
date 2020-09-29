/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.ClientNamenodeSCMProtocolProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.UnknownPipelineStateException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.security.token.Token;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class HDDSLocatedBlock extends LocatedBlock {
  @JsonProperty("blockId")
  private final BlockID blockID;
  // the id of this subkey in all the subkeys.
  private long length;
  private final long offset;
  // Block token, required for client authentication when security is enabled.
  private Token<OzoneBlockTokenIdentifier> token;
  // the version number indicating when this block was added
  private long createVersion;

  @JsonIgnore
  private Pipeline pipeline;

  public HDDSLocatedBlock(BlockID blockID, Pipeline pipeline, long length,
      long offset) {
    super(null, null);
    this.blockID = blockID;
    this.pipeline = pipeline;
    this.length = length;
    this.offset = offset;
  }

  public HDDSLocatedBlock(BlockID blockID, Pipeline pipeline, long length,
      long offset, Token<OzoneBlockTokenIdentifier> token) {
    super(null, null);
    this.blockID = blockID;
    this.pipeline = pipeline;
    this.length = length;
    this.offset = offset;
    this.token = token;
  }

  public void setCreateVersion(long version) {
    createVersion = version;
  }

  public long getCreateVersion() {
    return createVersion;
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

  @JsonIgnore
  public Pipeline getPipeline() {
    return pipeline;
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

  public Token<OzoneBlockTokenIdentifier> getToken() {
    return token;
  }

  public void setToken(Token<OzoneBlockTokenIdentifier> token) {
    this.token = token;
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder {
    private BlockID blockID;
    private long length;
    private long offset;
    private Token<OzoneBlockTokenIdentifier> token;
    private Pipeline pipeline;

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setPipeline(Pipeline pipeline) {
      this.pipeline = pipeline;
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

    public Builder setToken(Token<OzoneBlockTokenIdentifier> bToken) {
      this.token = bToken;
      return this;
    }

    public HDDSLocatedBlock build() {
      if (token == null) {
        return new HDDSLocatedBlock(blockID, pipeline, length, offset);
      } else {
        return new HDDSLocatedBlock(blockID, pipeline, length, offset, token);
      }
    }
  }

  @JsonIgnore
  public ClientNamenodeSCMProtocolProtos.HddsLocation getProtobuf() {
    ClientNamenodeSCMProtocolProtos.HddsLocation.Builder builder = ClientNamenodeSCMProtocolProtos.HddsLocation
        .newBuilder()
        .setBlockID(blockID.getProtobuf())
        .setLength(length)
        .setOffset(offset)
        .setCreateVersion(createVersion);
    if (this.token != null) {
//      builder.setToken(OzonePBHelper.protoFromToken(token));
    }
    try {
      builder.setPipeline(pipeline.getProtobufMessage());
    } catch (UnknownPipelineStateException e) {
      //TODO: fix me: we should not return KeyLocation without pipeline.
    }
    return builder.build();
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

  public static HDDSLocatedBlock getFromProtobuf(
      ClientNamenodeSCMProtocolProtos.HddsLocation keyLocation) {
    HDDSLocatedBlock info = new HDDSLocatedBlock(
        BlockID.getFromProtobuf(keyLocation.getBlockID()),
        getPipeline(keyLocation),
        keyLocation.getLength(),
        keyLocation.getOffset());
    if(keyLocation.hasToken()) {
//      info.token = (Token<OzoneBlockTokenIdentifier>)
//              OzonePBHelper.tokenFromProto(keyLocation.getToken());
    }
    info.setCreateVersion(keyLocation.getCreateVersion());
    return info;
  }

  @Override
  public String toString() {
    return "{blockID={containerID=" + blockID.getContainerID() +
        ", localID=" + blockID.getLocalID() + "}" +
        ", length=" + length +
        ", offset=" + offset +
        ", token=" + token +
        ", pipeline=" + pipeline +
        ", createVersion=" + createVersion + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HDDSLocatedBlock that = (HDDSLocatedBlock) o;
    return length == that.length &&
        offset == that.offset &&
        createVersion == that.createVersion &&
        Objects.equals(blockID, that.blockID) &&
        Objects.equals(token, that.token) &&
        Objects.equals(pipeline, that.pipeline);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockID, length, offset, token, createVersion,
        pipeline);
  }

  @JsonIgnore
  public List<DatanodeDetails> getDatanodeDetails() {
    if (getPipeline() == null) {
      return Collections.emptyList();
    }
    return getPipeline().getNodes();
  }

  public DatanodeInfo[] getLocations() {
    List<DatanodeDetails> dnList = getDatanodeDetails();
    DatanodeInfo[] datanodeInfos = new DatanodeInfo[dnList.size()];
    for (int i = 0; i < dnList.size(); i++) {
      DatanodeDetails dn  = dnList.get(i);
      datanodeInfos[i] = new DatanodeInfo.DatanodeInfoBuilder()
          .setHostName(dn.getHostName())
          .setDatanodeUuid(dn.getUuidString())
          .setIpAddr(dn.getIpAddress())
          .build();
    }
    return datanodeInfos;
  }

  @JsonGetter("blockId")
  public String getBlockId() {
    return blockID.toString();
  }
}

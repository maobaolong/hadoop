package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.ClientNamenodeSCMProtocolProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.UnknownPipelineStateException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.Token;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class HDDSServerLocationInfo implements Writable {
  private BlockID blockID;
  // the id of this subkey in all the subkeys.
  private long length;
  private long offset;
  // Block token, required for client authentication when security is enabled.
  private Token<OzoneBlockTokenIdentifier> token;
  // the version number indicating when this block was added
  private long createVersion;

  private Pipeline pipeline;

  public HDDSServerLocationInfo() {
  }

  private HDDSServerLocationInfo(BlockID blockID, Pipeline pipeline, long length,
      long offset, Token<OzoneBlockTokenIdentifier> token, long createVersion) {
    this.blockID = blockID;
    this.pipeline = pipeline;
    this.length = length;
    this.offset = offset;
    this.token = token;
    this.createVersion = createVersion;
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
    private long createVersion;

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

    public Builder setCreateVersion(long version) {
      this.createVersion = version;
      return this;
    }

    public HDDSServerLocationInfo build() {
      return new HDDSServerLocationInfo(blockID, pipeline, length,
          offset, token, createVersion);
    }
  }

  public HdfsProtos.HDDSServerLocationInfoProto getProtobuf() {
    return HdfsProtos.HDDSServerLocationInfoProto
        .newBuilder()
        .setContainerId(blockID.getContainerID())
        .setLocalId(blockID.getLocalID())
        .setLength(getLength())
        .setCreateVersion(createVersion)
        .setOffset(getOffset())
        .build();
  }

  public static HDDSServerLocationInfo getFromProtobuf(
      HdfsProtos.HDDSServerLocationInfoProto locationInfoProto) {
    return new HDDSServerLocationInfo.Builder()
        .setBlockID(new BlockID(locationInfoProto.getContainerId(),
            locationInfoProto.getLocalId()))
        .setLength(locationInfoProto.getLength())
        .setOffset(locationInfoProto.getOffset())
        .setCreateVersion(locationInfoProto.getCreateVersion())
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
        ", token=" + token +
        ", pipeline=" + pipeline +
        ", createVersion=" + createVersion + '}';
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
    out.writeLong(createVersion);
  }

  final void readHelper(DataInput in) throws IOException {
    long containerID = in.readLong();
    long localID = in.readLong();
    this.blockID = new BlockID(containerID, localID);
    this.length = in.readLong();
    this.offset = in.readLong();
    this.createVersion = in.readLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HDDSServerLocationInfo that = (HDDSServerLocationInfo) o;
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

  public long getNumBytes() {
    return length;
  }
}

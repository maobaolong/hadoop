package org.apache.hadoop.hdds;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HDDSLocatedBlocks extends LocatedBlocks {
  private final long fileLength;
  // array of blocks with prioritized locations
  private final List<LocatedBlock> blocks;
  private final boolean underConstruction;
  private final HDDSLocatedBlock lastLocatedBlock;
  private final boolean isLastBlockComplete;
  private final FileEncryptionInfo fileEncryptionInfo;
  private final ErasureCodingPolicy ecPolicy;

  public HDDSLocatedBlocks() {
    fileLength = 0;
    blocks = null;
    underConstruction = false;
    lastLocatedBlock = null;
    isLastBlockComplete = false;
    fileEncryptionInfo = null;
    ecPolicy = null;
  }

  public HDDSLocatedBlocks(long flength, boolean isUnderConstuction,
                       List<LocatedBlock> blks, HDDSLocatedBlock lastBlock,
                       boolean isLastBlockCompleted, FileEncryptionInfo feInfo,
                       ErasureCodingPolicy ecPolicy) {
    fileLength = flength;
    blocks = blks;
    underConstruction = isUnderConstuction;
    this.lastLocatedBlock = lastBlock;
    this.isLastBlockComplete = isLastBlockCompleted;
    this.fileEncryptionInfo = feInfo;
    this.ecPolicy = ecPolicy;
  }

  /**
   * Get located blocks.
   */
  public List<LocatedBlock> getLocatedBlocks() {
    return blocks;
  }

  /** Get the last located block. */
  public HDDSLocatedBlock getLastLocatedBlock() {
    return lastLocatedBlock;
  }

  /** Is the last block completed? */
  public boolean isLastBlockComplete() {
    return isLastBlockComplete;
  }

  /**
   * Get located block.
   */
  public LocatedBlock get(int index) {
    return blocks.get(index);
  }

  /**
   * Get number of located blocks.
   */
  public int locatedBlockCount() {
    return blocks == null ? 0 : blocks.size();
  }

  /**
   *
   */
  public long getFileLength() {
    return this.fileLength;
  }

  /**
   * Return true if file was under construction when this LocatedBlocks was
   * constructed, false otherwise.
   */
  public boolean isUnderConstruction() {
    return underConstruction;
  }

  /**
   * @return the FileEncryptionInfo for the LocatedBlocks
   */
  public FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  /**
   * @return The ECPolicy for ErasureCoded file, null otherwise.
   */
  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }

  /**
   * Find block containing specified offset.
   *
   * @return block if found, or null otherwise.
   */
  public int findBlock(long offset) {
    // create fake block of size 0 as a key
    LocatedBlock key = new HDDSLocatedBlock.Builder()
        .setOffset(offset)
        .setLength(1)
        .build();
    Comparator<LocatedBlock> comp =
        new Comparator<LocatedBlock>() {
          // Returns 0 iff a is inside b or b is inside a
          @Override
          public int compare(LocatedBlock a, LocatedBlock b) {
            long aBeg = ((HDDSLocatedBlock)a).getOffset();
            long bBeg = ((HDDSLocatedBlock)b).getOffset();
            long aEnd = aBeg + ((HDDSLocatedBlock)a).getLength();
            long bEnd = bBeg + ((HDDSLocatedBlock)b).getLength();
            if(aBeg <= bBeg && bEnd <= aEnd
                || bBeg <= aBeg && aEnd <= bEnd)
              return 0; // one of the blocks is inside the other
            if(aBeg < bBeg)
              return -1; // a's left bound is to the left of the b's
            return 1;
          }
        };
    return Collections.binarySearch(blocks, key, comp);
  }

  @Override
  public void insertRange(int blockIdx, List<LocatedBlock> newBlocks) {
    int oldIdx = blockIdx;
    int insStart = 0, insEnd = 0;
    for(int newIdx = 0; newIdx < newBlocks.size() && oldIdx < blocks.size();
        newIdx++) {
      HDDSLocatedBlock block = ((HDDSLocatedBlock)newBlocks.get(newIdx));
      long newOff = block.getOffset();
      long oldOff = ((HDDSLocatedBlock)blocks.get(oldIdx)).getOffset();
      if(newOff < oldOff) {
        insEnd++;
      } else if(newOff == oldOff) {
        // replace old cached block by the new one
        blocks.set(oldIdx, block);
        if(insStart < insEnd) { // insert new blocks
          blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
          oldIdx += insEnd - insStart;
        }
        insStart = insEnd = newIdx+1;
        oldIdx++;
      } else {  // newOff > oldOff
        assert false : "List of LocatedBlock must be sorted by startOffset";
      }
    }
    insEnd = newBlocks.size();
    if(insStart < insEnd) { // insert new blocks
      blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
    }
  }

  public static int getInsertIndex(int binSearchResult) {
    return binSearchResult >= 0 ? binSearchResult : -(binSearchResult+1);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + ";  fileLength=" + fileLength
        + ";  underConstruction=" + underConstruction
        + ";  blocks=" + blocks
        + ";  lastLocatedBlock=" + lastLocatedBlock
        + ";  isLastBlockComplete=" + isLastBlockComplete
        + ";  ecPolicy=" + ecPolicy + "}";
  }
}

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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.HDDSLocatedBlocks;
import org.apache.hadoop.hdds.HDDSLocatedBlock;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class HddsLocatedBlockBuilder {

  protected long flen;
  protected List<LocatedBlock> blocks = Collections.emptyList();
  protected boolean isUC;
  protected HDDSLocatedBlock last;
  protected boolean lastComplete;
  protected FileEncryptionInfo feInfo;
  private final int maxBlocks;
  protected ErasureCodingPolicy ecPolicy;

  HddsLocatedBlockBuilder(int maxBlocks) {
    this.maxBlocks = maxBlocks;
  }

  boolean isBlockMax() {
    return blocks.size() >= maxBlocks;
  }

  HddsLocatedBlockBuilder fileLength(long fileLength) {
    flen = fileLength;
    return this;
  }

  HddsLocatedBlockBuilder addBlock(HDDSLocatedBlock block) {
    if (blocks.isEmpty()) {
      blocks = new ArrayList<>();
    }
    blocks.add(block);
    return this;
  }

  HddsLocatedBlockBuilder lastUC(boolean underConstruction) {
    isUC = underConstruction;
    return this;
  }

  HddsLocatedBlockBuilder lastBlock(HDDSLocatedBlock block) {
    last = block;
    return this;
  }

  HddsLocatedBlockBuilder lastComplete(boolean complete) {
    lastComplete = complete;
    return this;
  }

  HddsLocatedBlockBuilder encryption(FileEncryptionInfo fileEncryptionInfo) {
    feInfo = fileEncryptionInfo;
    return this;
  }

  HddsLocatedBlockBuilder erasureCoding(ErasureCodingPolicy codingPolicy) {
    ecPolicy = codingPolicy;
    return this;
  }

  HDDSLocatedBlocks build(DatanodeDescriptor client) {
    return build();
  }

  HDDSLocatedBlocks build() {
    return new HDDSLocatedBlocks(flen, isUC, blocks, last,
        lastComplete, feInfo, ecPolicy);
  }
}

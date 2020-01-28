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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.metastore.InodeStore;
import org.apache.hadoop.hdfs.server.namenode.metastore.heap.HeapInodeStore;

/**
 * Storing all the {@link INode}s and maintaining the mapping between INode ID
 * and INode.  
*/
public class INodeMap {

  static INodeMap newInstance(INodeDirectory rootDir) {
    InodeStore inodeStore = new HeapInodeStore(rootDir);
    return new INodeMap(inodeStore);
  }

  /** Synchronized by external lock. */
  private InodeStore inodeStore;
  
  public Iterator<INodeWithAdditionalFields> getMapIterator() {
    return inodeStore.getMapIterator();
  }

  private INodeMap(InodeStore inodeStore) {
    this.inodeStore = inodeStore;
  }
  
  /**
   * Add an {@link INode} into the {@link INode} map. Replace the old value if 
   * necessary. 
   * @param inode The {@link INode} to be added to the map.
   */
  public final void put(INode inode) {
    // TODO(maobaolong): throw the exception to the callee
    try {
      inodeStore.put(inode);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Remove a {@link INode} from the map.
   * @param inode The {@link INode} to be removed.
   */
  public final void remove(INode inode) {
    // TODO(maobaolong): throw the exception to the callee
    try {
      inodeStore.remove(inode);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * @return The size of the map.
   */
  public int size() {
    return inodeStore.size();
  }
  
  /**
   * Get the {@link INode} with the given id from the map.
   * @param id ID of the {@link INode}.
   * @return The {@link INode} in the map with the given id. Return null if no 
   *         such {@link INode} in the map.
   */
  public INode get(long id) {
    INode inode = new INodeWithAdditionalFields(id, null, new PermissionStatus(
        "", "", new FsPermission((short) 0)), 0, 0) {
      
      @Override
      void recordModification(int latestSnapshotId) {
      }
      
      @Override
      public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
        // Nothing to do
      }

      @Override
      public QuotaCounts computeQuotaUsage(
          BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
          boolean useCache, int lastSnapshotId) {
        return null;
      }

      @Override
      public ContentSummaryComputationContext computeContentSummary(
          int snapshotId, ContentSummaryComputationContext summary) {
        return null;
      }
      
      @Override
      public void cleanSubtree(
          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
      }

      @Override
      public byte getStoragePolicyID(){
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }

      @Override
      public byte getLocalStoragePolicyID() {
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }
    };

    // TODO(maobaolong): throw the exception to the callee
    try {
      return inodeStore.get(inode);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  /**
   * Clear the {@link #inodeStore}
   */
  public void clear() {
    inodeStore.clear();
  }
}

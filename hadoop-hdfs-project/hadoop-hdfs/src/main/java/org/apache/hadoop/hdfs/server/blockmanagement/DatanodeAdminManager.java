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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * Manages decommissioning and maintenance state for DataNodes. A background
 * monitor thread periodically checks the status of DataNodes that are
 * decommissioning or entering maintenance state.
 * <p/>
 * A DataNode can be decommissioned in a few situations:
 * <ul>
 * <li>If a DN is dead, it is decommissioned immediately.</li>
 * <li>If a DN is alive, it is decommissioned after all of its blocks
 * are sufficiently replicated. Merely under-replicated blocks do not
 * block decommissioning as long as they are above a replication
 * threshold.</li>
 * </ul>
 * In the second case, the DataNode transitions to a DECOMMISSION_INPROGRESS
 * state and is tracked by the monitor thread. The monitor periodically scans
 * through the list of insufficiently replicated blocks on these DataNodes to
 * determine if they can be DECOMMISSIONED. The monitor also prunes this list
 * as blocks become replicated, so monitor scans will become more efficient
 * over time.
 * <p/>
 * DECOMMISSION_INPROGRESS nodes that become dead do not progress to
 * DECOMMISSIONED until they become live again. This prevents potential
 * durability loss for singly-replicated blocks (see HDFS-6791).
 * <p/>
 * DataNodes can also be put under maintenance state for any short duration
 * maintenance operations. Unlike decommissioning, blocks are not always
 * re-replicated for the DataNodes to enter maintenance state. When the
 * blocks are replicated at least dfs.namenode.maintenance.replication.min,
 * DataNodes transition to IN_MAINTENANCE state. Otherwise, just like
 * decommissioning, DataNodes transition to ENTERING_MAINTENANCE state and
 * wait for the blocks to be sufficiently replicated and then transition to
 * IN_MAINTENANCE state. The block replication factor is relaxed for a maximum
 * of maintenance expiry time. When DataNodes don't transition or join the
 * cluster back by expiry time, blocks are re-replicated just as in
 * decommissioning case as to avoid read or write performance degradation.
 * <p/>
 * This class depends on the FSNamesystem lock for synchronization.
 */
@InterfaceAudience.Private
public interface DatanodeAdminManager {
  /**
   * Start the DataNode admin monitor thread.
   * @param conf
   */
  void activate(Configuration conf);

  /**
   * Stop the admin monitor thread, waiting briefly for it to terminate.
   */
  void close();

  /**
   * Start decommissioning the specified datanode.
   * @param node
   */
  void startDecommission(DatanodeDescriptor node);

  /**
   * Stop decommissioning the specified datanode.
   * @param node
   */
  void stopDecommission(DatanodeDescriptor node);

  /**
   * Start maintenance of the specified datanode.
   * @param node
   */
  void startMaintenance(DatanodeDescriptor node,
      long maintenanceExpireTimeInMS);

  /**
   * Stop maintenance of the specified datanode.
   * @param node
   */
  void stopMaintenance(DatanodeDescriptor node);
}

package org.apache.hadoop.hdds;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsNamedFileStatus;

import java.util.EnumSet;
import java.util.Set;

public class HDDSFileStatus extends HdfsNamedFileStatus {
  HDDSLocatedBlocks locatedBlocks;

  public HDDSFileStatus(long length, boolean isdir, int replication,
                        long blocksize, long mtime, long atime,
                        FsPermission permission, Set<Flags> flags,
                        String owner, String group,
                        byte[] symlink, byte[] path, long fileId,
                        int childrenNum, FileEncryptionInfo feInfo,
                        byte storagePolicy, ErasureCodingPolicy ecPolicy,
                        HDDSLocatedBlocks locatedBlocks) {
    super(length, isdir, replication, blocksize, mtime, atime,
        permission, flags,
        owner, group, symlink, path, fileId,
        childrenNum, feInfo, storagePolicy, ecPolicy);
    this.locatedBlocks = locatedBlocks;
  }

  public static HDDSFileStatus fromHdfsFileStatus(HdfsFileStatus stat,
      HDDSLocatedBlocks locatedBlocks) {
    HDDSFileStatus hddsFileStatus = new HDDSFileStatus(stat.getLen(), !stat.isFile(),
        stat.getReplication(), stat.getBlockSize(), stat.getModificationTime(),
        stat.getAccessTime(),
        stat.getPermission(),
        // TODO(baoloongmao): copy flags from stat.
        EnumSet.noneOf(Flags.class),
        stat.getOwner(), stat.getGroup(),
        stat.getSymlinkInBytes(), stat.getLocalNameInBytes(), stat.getFileId(),
        stat.getChildrenNum(), stat.getFileEncryptionInfo(),
        stat.getStoragePolicy(), stat.getErasureCodingPolicy(), locatedBlocks);
    return hddsFileStatus;
  }

  public HDDSLocatedBlocks getLocatedBlocks() {
    return locatedBlocks;
  }
}

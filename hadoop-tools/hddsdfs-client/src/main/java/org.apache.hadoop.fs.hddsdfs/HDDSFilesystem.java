package org.apache.hadoop.fs.hddsdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsPathHandle;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

public class HDDSFilesystem extends DistributedFileSystem {

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(DFSOpsCountStatistics.OpType.OPEN);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataInputStream>() {
      @Override
      public FSDataInputStream doCall(final Path p) throws IOException {
        final HDDSInputStream hddsis = getHDDSClient().openHDDS(getPathName(p),
            bufferSize, true);
        return new FSDataInputStream(hddsis);
      }

      @Override
      public FSDataInputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.open(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    dfs = new HDDSClient(uri, conf, statistics);
  }

  public HDDSClient getHDDSClient() {
    return (HDDSClient) dfs;
  }

  @Override
  public String getScheme() {
    return "hddsfs";
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> cflags, final int bufferSize,
      final short replication, final long blockSize,
      final Progressable progress, final Options.ChecksumOpt checksumOpt)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(DFSOpsCountStatistics.OpType.CREATE);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream doCall(final Path p) throws IOException {
        final FSDataOutputStream fsos = new FSDataOutputStream(
            getHDDSClient().newStreamForCreate(
                getPathName(p), permission, cflags, true,
                replication, blockSize, progress,
                dfs.getConf().createChecksum(checksumOpt)), statistics);

        return fsos;
      }
      @Override
      public FSDataOutputStream next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.create(p, permission, cflags, bufferSize,
            replication, blockSize, progress, checksumOpt);
      }
    }.resolve(this, absF);
  }

  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize)
      throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(DFSOpsCountStatistics.OpType.OPEN);
    if (!(fd instanceof HdfsPathHandle)) {
      fd = new HdfsPathHandle(fd.bytes());
    }
    HdfsPathHandle id = (HdfsPathHandle) fd;
    final HDDSInputStream hddsis = getHDDSClient().openHDDS(id, bufferSize, true);
    return new FSDataInputStream(hddsis);
  }
}

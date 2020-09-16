package org.apache.hadoop.fs.hddsdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.HDDSFileStatus;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsPathHandle;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
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

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter)
      throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<LocatedFileStatus>>() {
      @Override
      public RemoteIterator<LocatedFileStatus> doCall(final Path p)
          throws IOException {
        return new DirListingIterator<>(p, filter, true);
      }

      @Override
      public RemoteIterator<LocatedFileStatus> next(final FileSystem fs,
          final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          return ((DistributedFileSystem)fs).listLocatedStatus(p, filter);
        }
        // symlink resolution for this methos does not work cross file systems
        // because it is a protected method.
        throw new IOException("Link resolution does not work with multiple " +
            "file systems for listLocatedStatus(): " + p);
      }
    }.resolve(this, absF);
  }


  /**
   * Returns a remote iterator so that followup calls are made on demand
   * while consuming the entries. This reduces memory consumption during
   * listing of a large directory.
   *
   * @param p target path
   * @return remote iterator
   */
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<FileStatus>>() {
      @Override
      public RemoteIterator<FileStatus> doCall(final Path p)
          throws IOException {
        return new DirListingIterator<>(p, false);
      }

      @Override
      public RemoteIterator<FileStatus> next(final FileSystem fs, final Path p)
          throws IOException {
        return ((DistributedFileSystem)fs).listStatusIterator(p);
      }
    }.resolve(this, absF);

  }

  /**
   * This class defines an iterator that returns
   * the file status of each file/subdirectory of a directory
   *
   * if needLocation, status contains block location if it is a file
   * throws a RuntimeException with the error as its cause.
   *
   * @param <T> the type of the file status
   */
  private class  DirListingIterator<T extends FileStatus>
      implements RemoteIterator<T> {
    private DirectoryListing thisListing;
    private int i;
    private Path p;
    private String src;
    private T curStat = null;
    private PathFilter filter;
    private boolean needLocation;

    private DirListingIterator(Path p, PathFilter filter,
        boolean needLocation) throws IOException {
      this.p = p;
      this.src = getPathName(p);
      this.filter = filter;
      this.needLocation = needLocation;
      // fetch the first batch of entries in the directory
      thisListing = dfs.listPaths(src, HdfsFileStatus.EMPTY_NAME,
          needLocation);
      statistics.incrementReadOps(1);
      if (needLocation) {
        storageStatistics.incrementOpCounter(DFSOpsCountStatistics.OpType.LIST_LOCATED_STATUS);
      } else {
        storageStatistics.incrementOpCounter(DFSOpsCountStatistics.OpType.LIST_STATUS);
      }
      if (thisListing == null) { // the directory does not exist
        throw new FileNotFoundException("File " + p + " does not exist.");
      }
      i = 0;
    }

    private DirListingIterator(Path p, boolean needLocation)
        throws IOException {
      this(p, null, needLocation);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() throws IOException {
      while (curStat == null && hasNextNoFilter()) {
        T next;
        HdfsFileStatus fileStat = thisListing.getPartialListing()[i++];
        if (needLocation) {
          next = (T)((HDDSFileStatus)fileStat)
              .makeQualifiedLocated(getUri(), p);
        } else {
          next = (T)fileStat.makeQualified(getUri(), p);
        }
        // apply filter if not null
        if (filter == null || filter.accept(next.getPath())) {
          curStat = next;
        }
      }
      return curStat != null;
    }

    /** Check if there is a next item before applying the given filter */
    private boolean hasNextNoFilter() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i >= thisListing.getPartialListing().length
          && thisListing.hasMore()) {
        // current listing is exhausted & fetch a new listing
        thisListing = dfs.listPaths(src, thisListing.getLastName(),
            needLocation);
        statistics.incrementReadOps(1);
        if (thisListing == null) {
          throw new FileNotFoundException("File " + p + " does not exist.");
        }
        i = 0;
      }
      return (i < thisListing.getPartialListing().length);
    }

    @Override
    public T next() throws IOException {
      if (hasNext()) {
        T tmp = curStat;
        curStat = null;
        return tmp;
      }
      throw new java.util.NoSuchElementException("No more entry in " + p);
    }
  }
}

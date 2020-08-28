package org.apache.hadoop.fs.hddsdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.htrace.core.TraceScope;
import org.apache.ratis.protocol.ClientId;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;

public class HDDSClient extends DFSClient {
  private static final int CREATE_RETRY_COUNT = 10;
  static CryptoProtocolVersion[] SUPPORTED_CRYPTO_VERSIONS =
      CryptoProtocolVersion.supported();

  private ReplicationType replicationType;
  private ReplicationFactor replicationFactor;
  private XceiverClientManager xceiverClientManager;
  private final int chunkSize;
  private final ContainerProtos.ChecksumType checksumType;
  private final int bytesPerChecksum;
  private boolean verifyChecksum;
  private final int streamBufferSize;
  private final long streamBufferFlushSize;
  private boolean streamBufferFlushDelay;
  private final long streamBufferMaxSize;
  private final long blockSize;
  private final ClientId clientId = ClientId.randomId();
  private final int maxRetryCount;
  private final long retryInterval;
  private Text dtService;
  private final boolean topologyAwareReadEnabled;
  private final boolean checkKeyNameEnabled;

  public HDDSClient(Configuration conf)
      throws IOException {
    this(DFSUtilClient.getNNAddress(conf), conf);
  }

  public HDDSClient(InetSocketAddress address,
                    Configuration conf) throws IOException {
    this(DFSUtilClient.getNNUri(address), conf);
  }

  public HDDSClient(URI nameNodeUri,
                    Configuration conf) throws IOException {
    this(nameNodeUri, conf, null);
  }

  public HDDSClient(URI nameNodeUri, Configuration conf,
                    FileSystem.Statistics stats)
      throws IOException {
    this(nameNodeUri, null, conf, stats);
  }

  public HDDSClient(URI nameNodeUri,
                    ClientProtocol rpcNamenode,
                    Configuration hadoopConf,
                    FileSystem.Statistics stats) throws IOException {
    super(nameNodeUri, rpcNamenode, hadoopConf, stats);
    OzoneConfiguration conf = OzoneConfiguration.of(hadoopConf);
    String replicationTypeConf =
        conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
            OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT);

    int replicationCountConf = conf.getInt(OzoneConfigKeys.OZONE_REPLICATION,
        OzoneConfigKeys.OZONE_REPLICATION_DEFAULT);
    this.replicationType = ReplicationType.valueOf(replicationTypeConf);
    this.replicationFactor = ReplicationFactor.valueOf(replicationCountConf);
    this.xceiverClientManager = new XceiverClientManager(conf,
        conf.getObject(XceiverClientManager.ScmClientConfig.class), "notImpl");


    int configuredChunkSize = (int) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
            ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    if(configuredChunkSize > OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE) {
      LOG.warn("The chunk size ({}) is not allowed to be more than"
              + " the maximum size ({}),"
              + " resetting to the maximum size.",
          configuredChunkSize, OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);
      chunkSize = OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE;
    } else {
      chunkSize = configuredChunkSize;
    }
    streamBufferSize = (int) conf
        .getStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_SIZE,
            OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_SIZE_DEFAULT,
            StorageUnit.BYTES);
    streamBufferFlushSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE,
            OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE_DEFAULT,
            StorageUnit.BYTES);
    streamBufferFlushDelay = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_DELAY,
        OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_DELAY_DEFAULT);
    streamBufferMaxSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE,
            OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE_DEFAULT,
            StorageUnit.BYTES);
    blockSize = (long) conf.getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    int configuredChecksumSize = (int) conf.getStorageSize(
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM,
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT,
        StorageUnit.BYTES);
    if(configuredChecksumSize <
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE) {
      LOG.warn("The checksum size ({}) is not allowed to be less than the " +
              "minimum size ({}), resetting to the minimum size.",
          configuredChecksumSize,
          OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE);
      bytesPerChecksum =
          OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE;
    } else {
      bytesPerChecksum = configuredChecksumSize;
    }

    String checksumTypeStr = conf.get(
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE,
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT);
    checksumType = ContainerProtos.ChecksumType.valueOf(checksumTypeStr);
    this.verifyChecksum =
        conf.getBoolean(OzoneConfigKeys.OZONE_CLIENT_VERIFY_CHECKSUM,
            OzoneConfigKeys.OZONE_CLIENT_VERIFY_CHECKSUM_DEFAULT);
    maxRetryCount =
        conf.getInt(OzoneConfigKeys.OZONE_CLIENT_MAX_RETRIES, OzoneConfigKeys.
            OZONE_CLIENT_MAX_RETRIES_DEFAULT);
    retryInterval = OzoneUtils.getTimeDurationInMS(conf,
        OzoneConfigKeys.OZONE_CLIENT_RETRY_INTERVAL,
        OzoneConfigKeys.OZONE_CLIENT_RETRY_INTERVAL_DEFAULT);
    topologyAwareReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
    checkKeyNameEnabled = conf.getBoolean(
        OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
        OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
  }

  @Override
  public DFSOutputStream create(String src, FsPermission permission,
                                EnumSet<CreateFlag> flag, boolean createParent, short replication,
                                long blockSize, Progressable progress, int buffersize,
                                Options.ChecksumOpt checksumOpt, InetSocketAddress[] favoredNodes,
                                String ecPolicyName) throws IOException {
    checkOpen();
    final FsPermission masked = applyUMask(permission);
    LOG.debug("{}: masked={}", src, masked);
    final DFSOutputStream result = DFSOutputStream.newStreamForCreate(this,
        src, masked, flag, createParent, replication, blockSize, progress,
        getConf().createChecksum(checksumOpt),
        getFavoredNodesStr(favoredNodes), ecPolicyName);

    beginFileLease(result.getFileId(), result);
    return result;
  }


  public HDDSOutputStream newStreamForCreate(String src,
                                             FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
                                             short replication, long blockSize, Progressable progress,
                                             DataChecksum checksum)
      throws IOException {
    try (TraceScope ignored =
             newPathTraceScope("newStreamForCreate", src.toString())) {
      HdfsFileStatus stat = null;

      masked = applyUMask(masked);
      // Retry the create if we get a RetryStartFileException up to a maximum
      // number of times
      boolean shouldRetry = true;
      int retryCount = CREATE_RETRY_COUNT;
      while (shouldRetry) {
        shouldRetry = false;
        try {
          stat = getNamenode().create(src, masked, getClientName(),
              new EnumSetWritable<>(flag), createParent, replication,
              blockSize, SUPPORTED_CRYPTO_VERSIONS, "");
          break;
        } catch (RemoteException re) {
          IOException e = re.unwrapRemoteException(
              AccessControlException.class,
              DSQuotaExceededException.class,
              QuotaByStorageTypeExceededException.class,
              FileAlreadyExistsException.class,
              FileNotFoundException.class,
              ParentNotDirectoryException.class,
              NSQuotaExceededException.class,
              RetryStartFileException.class,
              SafeModeException.class,
              UnresolvedPathException.class,
              SnapshotAccessControlException.class,
              UnknownCryptoProtocolVersionException.class);
          if (e instanceof RetryStartFileException) {
            if (retryCount > 0) {
              shouldRetry = true;
              retryCount--;
            } else {
              throw new IOException("Too many retries because of encryption" +
                  " zone operations", e);
            }
          } else {
            throw e;
          }
        }
      }
      Preconditions.checkNotNull(stat, "HdfsFileStatus should not be null!");
      HDDSOutputStream hddsout = new HDDSOutputStream.Builder()
          .setXceiverClientManager(xceiverClientManager)
          .setOmClient(this)
          .setSrc(src)
          .setChunkSize(chunkSize)
          .setRequestID(UUID.randomUUID().toString())
          .setType(HddsProtos.ReplicationType.valueOf(replicationType.toString()))
          .setFactor(HddsProtos.ReplicationFactor.valueOf(replicationFactor.getValue()))
          .setStreamBufferSize(streamBufferSize)
          .setStreamBufferFlushSize(streamBufferFlushSize)
          .setStreamBufferFlushDelay(streamBufferFlushDelay)
          .setStreamBufferMaxSize(streamBufferMaxSize)
          .setBlockSize(blockSize)
          .setChecksumType(checksumType)
          .setBytesPerChecksum(bytesPerChecksum)
          .setMaxRetryCount(maxRetryCount)
          .setRetryInterval(retryInterval)
          .build();
      return hddsout;
    }
  }



}

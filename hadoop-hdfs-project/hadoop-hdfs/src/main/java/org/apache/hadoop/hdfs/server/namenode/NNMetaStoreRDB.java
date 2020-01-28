package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class NNMetaStoreRDB {
    private static final String INODE_TABLE_NAME = "inodeTable";
    private static final String TMP_DB_NAME = "namenode.db";
    private static final Logger LOG =
            LoggerFactory.getLogger(NNMetaStoreRDB.class);
    private DBStore tmpStore;

    private final OzoneConfiguration configuration;

    public NNMetaStoreRDB(OzoneConfiguration configuration)
            throws IOException {
        this.configuration = configuration;
        start();
    }

    private Table<INode, INodeWithAdditionalFields> inodeTable;

    public void start() throws IOException {
        if (this.tmpStore == null) {
            // TODO(shenlong): remove the hardcode
            File metaDir = new File("./db/");
            FileUtils.forceMkdir(metaDir);
            FileUtils.deleteQuietly(new File(metaDir + File.separator + TMP_DB_NAME));

            this.tmpStore = DBStoreBuilder.newBuilder(configuration)
                    .setName(TMP_DB_NAME)
                    .setPath(Paths.get(metaDir.getPath()))
                    .addTable(INODE_TABLE_NAME)
                    .addCodec(Integer.class, new IntegerCodec())
                    .build();
            this.inodeTable = tmpStore.getTable(INODE_TABLE_NAME, INode.class, INodeWithAdditionalFields.class);
            checkTableStatus(this.inodeTable, INODE_TABLE_NAME);
        }
    }

    public void stop() throws Exception {
        if (tmpStore != null) {
            tmpStore.close();
            tmpStore = null;
        }
    }

    private void checkTableStatus(Table table, String name) throws IOException {
        String logMessage = "Unable to get a reference to %s table. Cannot "
                + "continue.";
        String errMsg = "Inconsistent DB state, Table - %s. Please check the"
                + " logs for more info.";
        if (table == null) {
            LOG.error(String.format(logMessage, name));
            throw new IOException(String.format(errMsg, name));
        }
    }
}

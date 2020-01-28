package org.apache.hadoop.hdfs.server.namenode.metastore.rocks;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.metastore.InodeStore;

import java.io.IOException;
import java.util.Iterator;

public class RocksInodeStore implements InodeStore {
    private Table<INode, INodeWithAdditionalFields> inodeTable;

    @Override
    public void put(INode inode) throws IOException {
        if (inode instanceof INodeWithAdditionalFields) {
            inodeTable.put(inode, (INodeWithAdditionalFields) inode);
        }
    }

    @Override
    public INode get(INode inode) throws IOException {
        return inodeTable.get(inode);
    }

    @Override
    public void remove(INode inode) throws IOException {
        inodeTable.delete(inode);
    }

    @Override
    public int size() {
        // TODO(shenlong): to be impl.
        return 0;
    }

    @Override
    public void clear() {
        // TODO(shenlong): to be impl.
        // delete all the entries in rocksdb , so we should be deleting using DeleteFilesInRange()
    }

    @Override
    public Iterator<INodeWithAdditionalFields> getMapIterator() {
        // TODO(shenlong): to be impl graceful.
        TableIterator<INode, ? extends Table.KeyValue<INode, INodeWithAdditionalFields>> iterator = inodeTable.iterator();
        return new Iterator<INodeWithAdditionalFields>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public INodeWithAdditionalFields next() {
                try {
                    return iterator.next().getValue();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }
}

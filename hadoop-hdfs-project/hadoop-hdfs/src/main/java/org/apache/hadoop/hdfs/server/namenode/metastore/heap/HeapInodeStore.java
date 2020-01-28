package org.apache.hadoop.hdfs.server.namenode.metastore.heap;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.metastore.InodeStore;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import java.io.IOException;
import java.util.Iterator;

public class HeapInodeStore implements InodeStore {
    private final GSet<INode, INodeWithAdditionalFields> map;

    public HeapInodeStore(INodeDirectory rootDir) {
        int capacity = LightWeightGSet.computeCapacity(1, "INodeMap");
        GSet<INode, INodeWithAdditionalFields> map =
                new LightWeightGSet<>(capacity);
        map.put(rootDir);
        this.map = map;
    }

    @Override
    public void put(INode inode) throws IOException {
        if (inode instanceof INodeWithAdditionalFields) {
            map.put((INodeWithAdditionalFields) inode);
        }
    }

    @Override
    public INode get(INode inode) {
        return map.get(inode);
    }

    @Override
    public void remove(INode inode) throws IOException {
        map.remove(inode);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Iterator<INodeWithAdditionalFields> getMapIterator() {
        return map.iterator();
    }
}

package org.apache.hadoop.hdfs.server.namenode.metastore;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

import java.io.IOException;
import java.util.Iterator;

public interface InodeStore {
  void put(INode inode) throws IOException;
  INode get(INode inode) throws IOException;
  void remove(INode inode) throws IOException;
  int size();
  void clear();
  Iterator<INodeWithAdditionalFields> getMapIterator();
}

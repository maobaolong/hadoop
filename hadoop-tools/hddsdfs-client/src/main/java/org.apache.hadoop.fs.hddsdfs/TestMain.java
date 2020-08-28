package org.apache.hadoop.fs.hddsdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;

public class TestMain {
  public static void main(String[] args) throws Exception {
    FsShell fsShell = new FsShell(new Configuration());
    fsShell.run(new String[] { "-copyFromLocal", "/tmp/a.txt",
        "hddsfs://localhost:9820/3.txt"});
  }
}

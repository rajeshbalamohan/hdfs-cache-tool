package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

/**
 * <pre>
 * Simple tool to do the following
 * 1. Given a HDFS directory/pattern, iterate through its subfolders and cache them in HDFS
 * 2. Optionally create cache pool
 * </pre>
 */
public class HDFSCache extends Configured implements Tool {

  private static final String HDFS_LOC = "path";
  private static final String CACHE_POOL_NAME = "poolName";
  private static final String EXPIRATION_INTERVAL_MS = "ttl";
  private static final String VERBOSE = "verbose";

  private List<Path> path = new LinkedList<Path>();
  private String poolName;
  private Configuration conf;
  private DistributedFileSystem dfs;
  private long expirationIntervalMs;
  private boolean verbose = false;

  public void init() throws IOException {
    this.conf = getConf();
    Iterators.all(conf.getTrimmedStringCollection(HDFS_LOC).iterator(),
      new Predicate<String>() {
        public boolean apply(String pathStr) {
          return path.add(new Path(pathStr));
        }
      });
    this.poolName = conf.get(CACHE_POOL_NAME);
    this.expirationIntervalMs = conf.getLong(EXPIRATION_INTERVAL_MS, -1);
    this.verbose = conf.getBoolean(VERBOSE, false);

    if (path.isEmpty()) {
      printUsage();
    }
    dfs = (DistributedFileSystem) path.get(0).getFileSystem(conf);
    initiCachePool();
  }

  private List<FileStatus> getFiles() throws IOException {
    List<FileStatus> result = new LinkedList<FileStatus>();
    for (Path p : path) {
      result.addAll(Arrays.asList(dfs.globStatus(p)));
    }
    return result;
  }

  private void startCaching() throws IOException {
    long start = System.currentTimeMillis();
    List<FileStatus> fileStatusList = getFiles();
    for (FileStatus fileStatus : fileStatusList) {
      dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPath(fileStatus.getPath())
        .setPool(poolName)
        .setExpiration(
          (expirationIntervalMs > 0) ? Expiration
            .newRelative(expirationIntervalMs) : Expiration.NEVER).build());
      if (verbose) {
        System.out.println("Cached : " + fileStatus.getPath());
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Time taken to cache " + fileStatusList.size() + "="
        + (end - start) + " ms");
  }

  /**
   * Get cachepool
   * 
   * @throws IOException
   */
  private void initiCachePool() throws IOException {
    if (Strings.isNullOrEmpty(poolName)) {
      printUsage();
    }
    CachePoolEntry result = null;
    RemoteIterator<CachePoolEntry> it = dfs.listCachePools();
    while (it.hasNext()) {
      CachePoolEntry entry = it.next();
      if (poolName.equals(entry.getInfo().getPoolName())) {
        result = entry;
        break;
      }
    }
    if (result == null) {
      // create a cache pool
      dfs.addCachePool(new CachePoolInfo(poolName));
    }
  }

  public int run(String[] args) throws ParseException, IOException {
    init();
    startCaching();
    if (dfs != null) {
      dfs.close();
    }
    return 0;
  }

  private static void printUsage() {
    StringBuilder sb = new StringBuilder();
    sb.append("hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=<globPath; supports csv format>"
        + " -DpoolName=<poolName> -Dttl=<ttl_in_ms (0) to indicate NEVER> -Dverbose=true/false(false by default)");

    sb.append("\n").append("path, poolName are mandatory parameters");
    sb.append("\n")
      .append(
        "e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/*,/user/hive/*/2/* -DpoolName=test -Dverbose=true");
    sb.append("\n")
      .append(
        "e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/* -DpoolName=test -Dverbose=true");
    sb.append("\n")
      .append(
        "e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/* -DpoolName=test -Dttl=-1");
    sb.append("\n")
      .append(
        "e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/* -DpoolName=test -Dttl=100");
    System.out.println(sb.toString());
    System.exit(-1);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new HDFSCache(), args);
    System.exit(res);
  }
}

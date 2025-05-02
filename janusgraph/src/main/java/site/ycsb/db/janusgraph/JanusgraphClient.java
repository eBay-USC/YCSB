/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.janusgraph;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.script.Bindings;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.nugraph.client.config.AbstractNuGraphConfig;
import org.nugraph.client.config.NuGraphConfigManager;
import org.nugraph.client.gremlin.driver.remote.NuGraphClientException;
import org.nugraph.client.gremlin.driver.remote.Options;
import org.nugraph.client.gremlin.driver.remote.ReadMode;
import org.nugraph.client.gremlin.process.traversal.dsl.graph.RemoteNuGraphTraversalSource;
import org.nugraph.client.gremlin.structure.RemoteNuGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
* Janusgraph client for YCSB framework with CSV query reader.
*/
public class JanusgraphClient extends DB {
  private static final String HOST_NAME_DEFAULT = "nugraphservice-testyimingfdbntypes2-lvs-internal.vip.ebay.com";
  private static final String HOST_NAME = "nugraph.hostname";
  private static final String AUTH_OVERRIDE = "nugraph.authorityoverride";
  private static final String AUTH_OVERRIDE_DEFAULT = "nugraphservice-slc.monstor-preprod.svc.23.tess.io";
  private static final String KEYSPACE = "nugraph.keyspace";
  private static final String KEYSPACE_DEFAULT = "ldbc_sf_01_b";
  // CSV 文件所在目录配置属性
  private static final String CSV_DIRECTORY = "nugraph.csvdirectory";
  private static final String CSV_DIRECTORY_DEFAULT = "/data/";
  private static final String USE_CACHE = "nugraph.usecache";
  private static final String USE_CACHE_DEFAULT = "true";
  private static final String QUERY_TYPE = "nugraph.querytype";
  private static final String QUERY_TYPE_DEFAULT = "0";

  // 使用 BlockingQueue 替代自定义的 QueryBuffer，设置一个合适的容量（例如：1,000,000 条记录）
  private static final BlockingQueue<String> QUERY = new LinkedBlockingQueue<>(1000000);

  // 静态共享的 CSV 读取线程
  private static Thread csvReaderThread;
  // 静态共享的队列监控线程，用于定期打印队列大小
  private static Thread queueMonitorThread;

  private static RemoteNuGraphTraversalSource g;
  private GremlinGroovyScriptEngine gremlinEngine;
  private Bindings bindings;
  private static Logger logger = LoggerFactory.getLogger(JanusgraphClient.class);
  private String usecache;
  private String querytype;

  public synchronized RemoteNuGraphTraversalSource getInstance(String hostName, 
      String authOverride, String keyspace) {
    if (g == null) {
      try {
        AbstractNuGraphConfig config = new CustomNuGraphConfig(
            hostName, hostName, true, authOverride
        );
        NuGraphConfigManager.setDefaultConfigAndInit("YCSB", config);

        // 创建远程图遍历源
        HashMap<String, Object> optionsMap = new HashMap<>();
        optionsMap.put(Options.TIMEOUT_IN_MILLIS, 99999);
        optionsMap.put(Options.IS_RETRY_ALLOWED, true);
        optionsMap.put(Options.READ_MODE, ReadMode.READ_SNAPSHOT);
        g = RemoteNuGraph.instance().traversal().withRemote(keyspace, optionsMap);
      } catch (NuGraphClientException e) {
        throw new RuntimeException(e);
      }
    }
    return g;
  }

  /**
  * process 方法：执行 Gremlin 脚本，并将结果填充到传入的 result 列表中.
  */
  private Status process(String line, int repeat, List<?> result) {
    String script = line.split(" \\| ")[0];
    if (script.startsWith("g.")) {
      script = "g.with(\"cache\", " + usecache + ")." + script.substring(2);
    }
    System.out.println(script);
    for (int i = 0; i < repeat; ++i) {
      final Object scriptResult;
      GraphTraversal gt = null;
      try {
        scriptResult = gremlinEngine.eval(script, bindings);
        if (scriptResult instanceof GraphTraversal) {
          gt = (GraphTraversal) scriptResult;
          // 填充传入的 result 列表
          gt.fill(result);
        }
        break;
      } catch (Throwable e) {
        logger.error("Got exception", e);
        System.out.println("Could not evaluate script");
        e.printStackTrace();
        return Status.ERROR;
      } finally {
        if (gt != null) {
          try {
            gt.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    return Status.OK;
  }

  @Override
  public void init() throws DBException {
    try {
      final Properties props = getProperties();
      String hostname = props.getProperty(HOST_NAME, HOST_NAME_DEFAULT);
      String authOverride = props.getProperty(AUTH_OVERRIDE, AUTH_OVERRIDE_DEFAULT);
      String keyspace = props.getProperty(KEYSPACE, KEYSPACE_DEFAULT);
      usecache = props.getProperty(USE_CACHE, USE_CACHE_DEFAULT);
      querytype = props.getProperty(QUERY_TYPE, QUERY_TYPE_DEFAULT);

      System.out.println("hostname: " + hostname);
      System.out.println("auth: " + authOverride);
      System.out.println("keyspace: " + keyspace);

      getInstance(hostname, authOverride, keyspace);

      gremlinEngine = new GremlinGroovyScriptEngine();
      bindings = gremlinEngine.createBindings();
      bindings.put("g", g);

      // 启动共享 CSV 查询读取线程（多个 JanusgraphClient 实例共用）
      synchronized (JanusgraphClient.class) {
        if (csvReaderThread == null || !csvReaderThread.isAlive()) {
          String csvDir = props.getProperty(CSV_DIRECTORY, CSV_DIRECTORY_DEFAULT);
          csvReaderThread = new CSVQueryReaderThread(csvDir, QUERY, querytype);
          csvReaderThread.setDaemon(true);
          csvReaderThread.start();
        }
        
        // 启动队列监控线程，定期打印队列大小
        if (queueMonitorThread == null || !queueMonitorThread.isAlive()) {
          queueMonitorThread = new Thread(() -> {
              while (!Thread.currentThread().isInterrupted()) {
                System.out.println("Queue size: " + QUERY.size());
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            }
          );
          queueMonitorThread.setDaemon(true);
          queueMonitorThread.start();
        }
      }
      
    } catch (Exception e) {
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      // 关闭图遍历源及相关服务
      g.close();
      NuGraphConfigManager.shutdownService();
      // 注意：共享的 CSV 读取线程和队列监控线程不在此处中断，以便其他实例继续使用
      String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH:mm:ss").format(new java.util.Date());
      System.out.println("Finish running at " + timeStamp);
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "cleanup").getMessage(), e);
      throw new DBException(e);
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.OK;
  }

  /**
  * multiget 不再使用外部输入，而是从共享缓冲区中获取 query.
  */
  @Override
  public Status multiget(String[] ignored, List<?> result) {
    try {
      // 从共享缓冲区中获取一个 query（若为空则等待）
      String query = QUERY.take();
      ignored[0] = query;
      return process(query, 1, result);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status manyget(String query, Map<String, Map<String, ByteIterator>> result) {
    return Status.OK;
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }
  
  /**
  * 静态内部线程：从指定目录下读取所有 CSV 文件，打乱顺序后逐行读取 query 字段，
  * 并将每个 query 放入共享 BlockingQueue.
  */
  private static class CSVQueryReaderThread extends Thread {
    private final String directoryPath;
    private final BlockingQueue<String> queryQueue;
    private final String queryType;

    public CSVQueryReaderThread(String directoryPath, BlockingQueue<String> queryQueue, String queryType) {
      this.queryType = queryType;
      this.directoryPath = directoryPath;
      this.queryQueue = queryQueue;
    }



    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        File dir = new File(directoryPath);
        File[] csvFiles = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".csv"));
        if (csvFiles == null || csvFiles.length == 0) {
          try {
            Thread.sleep(1000); // 无 CSV 文件时等待
          } catch (InterruptedException e) {
            break;
          }
          continue;
        }
        List<File> fileList = Arrays.asList(csvFiles);
        Collections.shuffle(fileList);
        for (File csvFile : fileList) {
          System.out.println("Reading File: " + csvFile.getName());
          try (Reader in = new FileReader(csvFile)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader("seconds", "query", "queryDurationMs")
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
              // 获取每行的 query 字段，并加入队列
              String query = record.get("query");
              if (queryType.contains("cache")) {
                if(query.contains("probability")) {
                  queryQueue.put(query);
                }
              } else if (queryType.contains("1")) {
                if (query.contains("__.outE(\"related_aspect\")")) {
                  queryQueue.put(query);
                }
              } else if (queryType.contains("2")) {
                if (query.contains("__.outE(\"item2item\")") && query.contains("probability")) {
                  queryQueue.put(query);
                }
              } else if (queryType.contains("5")) {
                if (query.contains("__.outE(\"inventory_embedding\")") && query.contains("probability") && query.contains("1")) {
                  queryQueue.put(query);
                }
              } else {
                queryQueue.put(query);
              }
            }
          } catch (Exception e) {
            logger.error("Error reading CSV file: " + csvFile.getAbsolutePath(), e);
          }
        }
      }
    }
  }
}

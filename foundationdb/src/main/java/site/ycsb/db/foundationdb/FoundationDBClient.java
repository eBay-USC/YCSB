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

package site.ycsb.db.foundationdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import site.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * FoundationDB client for YCSB framework.
 */

public class FoundationDBClient extends DB {
  private FDB fdb;
  private Database db;
  private String dbName;
  private int batchSize;
  private int batchCount;
  private static final String API_VERSION          = "foundationdb.apiversion";
  private static final String SUSPACE              = "foundationdb.subspace";
  private static final String SUBSPACE_DEFAULT     = "cache";
  private static final String API_VERSION_DEFAULT  = "710";
  private static final String CLUSTER_FILE         = "foundationdb.clusterfile";
  private static final String CLUSTER_FILE_DEFAULT = "/etc/foundationdb/fdb.cluster";
  private static final String DB_NAME              = "foundationdb.dbname";
  private static final String DB_NAME_DEFAULT      = "DB";
  private static final String DB_BATCH_SIZE_DEFAULT = "0";
  // private static final String READ_VERSION         ="foundationdb.readversion";
  // private static final String READ_VERSION_DEFAULT = "fresh";   
  private static final String DB_BATCH_SIZE         = "foundationdb.batchsize";
  private Vector<String> batchKeys;
  private Vector<Map<String, ByteIterator>> batchValues;
  private DirectorySubspace subspace;
  private FoundationDBGetReadVersionWorker worker;
  // private boolean useNewestVersion; 
  private static Logger logger = LoggerFactory.getLogger(FoundationDBClient.class);
  private static synchronized  FDB getOrSelectFdb(int version){
    if(FDB.isAPIVersionSelected()) {
      return FDB.instance();
    }
    return FDB.selectAPIVersion(version);
  }
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // initialize FoundationDB driver
    final Properties props = getProperties();
    String subspaceType = props.getProperty(SUSPACE, SUBSPACE_DEFAULT);
    System.out.println(subspaceType);
    String apiVersion = props.getProperty(API_VERSION, API_VERSION_DEFAULT);
    String clusterFile = props.getProperty(CLUSTER_FILE, CLUSTER_FILE_DEFAULT);
    String dbBatchSize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);
    // String readVersion = props.getProperty(READ_VERSION, READ_VERSION_DEFAULT);
    // useNewestVersion = "fresh".equals(readVersion)?true:false;
    dbName = props.getProperty(DB_NAME, DB_NAME_DEFAULT);
    logger.info("API Version: {}", apiVersion);
    logger.info("Cluster File: {}\n", clusterFile);
    String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH:mm:ss").format(new java.util.Date());
    System.out.println("start running at "+timeStamp);
    try {
      
      fdb = getOrSelectFdb(710);
      db = fdb.open(clusterFile);
      batchSize = Integer.parseInt(dbBatchSize);
      batchCount = 0;
      batchKeys = new Vector<String>(batchSize+1);
      batchValues = new Vector<Map<String, ByteIterator>>(batchSize+1);
      if(subspaceType.equals("cache")){
        subspace = DirectoryLayer.getDefault().createOrOpenCache(db, PathUtil.from("cache")).join();
        System.out.println("Open cache subspace");
      } else {
        subspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from("normal")).join();
        System.out.println("Open normal subspace");
      }  
      worker = new FoundationDBGetReadVersionWorker(db, 200);
      worker.start();
      Thread.sleep(200);
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "init").getMessage(), e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      logger.error(MessageFormatter.format("Invalid value for apiversion property: {}", apiVersion).getMessage(), e);
      throw new DBException(e);
    } catch (InterruptedException e) {
      logger.error(MessageFormatter.format("Invalid value for readversion thread: {}", apiVersion).getMessage(), e);
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (batchCount > 0) {
      batchInsert();
      batchCount = 0;
    }
    try {
      worker.stopRunning();
      worker.join();
      db.close();
      String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH:mm:ss").format(new java.util.Date());
      System.out.println("Finish running at "+timeStamp);
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "cleanup").getMessage(), e);
      throw new DBException(e);
    } catch (InterruptedException e) {
      logger.error(MessageFormatter.format("Error in worker operation: {}", "cleanup").getMessage(), e);
      throw new DBException(e);

    }
  }

  private static String getRowKey(String db, String table, String key) {
    //return key + ":" + table + ":" + db;
    return db + ":" + table + ":" + key;
  }

  private static String getEndRowKey(String table) {
    return table + ";";
  }

  private Status convTupleToMap(Tuple tuple, Set<String> fields, Map<String, ByteIterator> result) {
    for (int i = 0; i < tuple.size(); i++) {
      Tuple v = tuple.getNestedTuple(i);
      String field = v.getString(0);
      String value = v.getString(1);
      //System.err.println(field + " : " + value);
      result.put(field, new StringByteIterator(value));
    }
    if (fields != null) {
      for (String field : fields) {
        if (result.get(field) == null) {
          logger.debug("field not fount: {}", field);
          return Status.NOT_FOUND;
        }
      }
    }
    return Status.OK;
  }

  private void batchInsert() {
    try {
      db.run(tr -> {
          for (int i = 0; i < batchCount; ++i) {
            Tuple t = new Tuple();
            for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(batchValues.get(i)).entrySet()) {
              Tuple v = new Tuple();
              v = v.add(entry.getKey());
              v = v.add(entry.getValue());
              t = t.add(v);
            }
           
            byte[]cacheKey = subspace.pack(Tuple.from(batchKeys.get(i)));
            tr.set(cacheKey, t.pack());
          }
          return null;
        });
    } catch (FDBException e) {
      for (int i = 0; i < batchCount; ++i) {
        logger.error(MessageFormatter.format("Error batch inserting key {}", batchKeys.get(i)).getMessage(), e);
      }
      e.printStackTrace();
    } catch (Throwable e) {
      for (int i = 0; i < batchCount; ++i) {
        logger.error(MessageFormatter.format("Error batch inserting key {}", batchKeys.get(i)).getMessage(), e);
      }
      e.printStackTrace();
    } finally {
      batchKeys.clear();
      batchValues.clear();
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("insert key = {}", rowKey);
    try {
      batchKeys.addElement(rowKey);
      batchValues.addElement(new HashMap<String, ByteIterator>(values));
      batchCount++;
      if (batchSize == 0 || batchSize == batchCount) {
        batchInsert();
        batchCount = 0;
      }
      return Status.OK;
    } catch (Throwable e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("delete key = {}", rowKey);
    try {
      db.run(tr -> {
          // byte[]cacheKey = ByteArrayUtil.join(CACHE_PREFIX, Tuple.from(rowKey).pack());
          byte[]cacheKey =  subspace.pack(Tuple.from(rowKey).pack());
          tr.clear(cacheKey);
          return null;
        });
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("read key = {}", rowKey);
    try {
      byte[] row = db.run(tr -> {
          //tr.setReadVersion(worker.getReadVersion());
          // byte[]cacheKey = ByteArrayUtil.join(CACHE_PREFIX, Tuple.from(rowKey).pack());
          byte[]cacheKey =  subspace.pack(Tuple.from(rowKey));
          byte[] r = tr.get(cacheKey).join();
          return r;
        });
      Tuple t = Tuple.fromBytes(row);
      if (t.size() == 0) {
        logger.debug("key not fount: {}", rowKey);
        return Status.NOT_FOUND;
      }
      return convTupleToMap(t, fields, result);
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("update key = {}", rowKey);
    try {
      Status s = db.run(tr -> {
          // byte[]cacheKey = ByteArrayUtil.join(CACHE_PREFIX, Tuple.from(rowKey).pack());
          byte[]cacheKey =  subspace.pack(Tuple.from(rowKey));
          byte[] row = tr.get(cacheKey).join();
          Tuple o = Tuple.fromBytes(row);
          if (o.size() == 0) {
            logger.debug("key not fount: {}", ByteArrayUtil.printable(cacheKey));
            return Status.NOT_FOUND;
          }
          HashMap<String, ByteIterator> result = new HashMap<>();
          if (convTupleToMap(o, null, result) != Status.OK) {
            return Status.ERROR;
          }
          for (String k : values.keySet()) {
            if (result.containsKey(k)) {
              result.put(k, values.get(k));
            } else {
              logger.debug("field not fount: {}", k);
              return Status.NOT_FOUND;
            }
          }
          Tuple t = new Tuple();
          for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(result).entrySet()) {
            Tuple v = new Tuple();
            v = v.add(entry.getKey());
            v = v.add(entry.getValue());
            t = t.add(v);
          }
          tr.set(cacheKey, t.pack());
          return Status.OK;
        });
      return s;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error updating key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    String startRowKey = getRowKey(dbName, table, startkey);
    byte[]startCacheKey =  subspace.pack(Tuple.from(startRowKey));
    String endRowKey = getEndRowKey(table);
    byte[]endCacheKey = subspace.pack(Tuple.from(endRowKey));
    logger.debug("scan key from {} to {} limit {} ", 
          ByteArrayUtil.printable(startCacheKey), ByteArrayUtil.printable(endCacheKey), recordcount);
    try (Transaction tr = db.createTransaction()) {
      tr.options().setReadYourWritesDisable();
      //tr.setReadVersion(worker.getReadVersion());
      AsyncIterable<KeyValue> entryList = tr.getRange(startCacheKey, endCacheKey,
          recordcount > 0 ? recordcount : 0);
      List<KeyValue> entries = entryList.asList().join();
      for (int i = 0; i < entries.size(); ++i) {
        final HashMap<String, ByteIterator> map = new HashMap<>();
        Tuple value = Tuple.fromBytes(entries.get(i).getValue());
        if (convTupleToMap(value, fields, map) == Status.OK) {
          result.add(map);
        } else {
          logger.error("Error scanning keys: from {} to {} limit {} ", 
                ByteArrayUtil.printable(startCacheKey), ByteArrayUtil.printable(endCacheKey), recordcount);
          return Status.ERROR;
        }
      }
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}

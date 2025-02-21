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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * FoundationDB client for YCSB framework.
 */

public class FoundationDBClient extends DB {
  private static final String API_ADDRESS          = "gremlin.address";
  private static final String API_PORT             = "gremlin.port";
  private Cluster cluster;
  private Client client;
  private static Logger logger = LoggerFactory.getLogger(FoundationDBClient.class);

  public List<Result> executeQuery(String gremlinQuery) {
    ResultSet resultSet = client.submit(gremlinQuery);
    return resultSet.all().join();
  }
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // initialize FoundationDB driver
    final Properties props = getProperties();
    try {
      cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
      client = cluster.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    // String apiVersion = props.getProperty(API_VERSION, API_VERSION_DEFAULT);
  }

  @Override
  public void cleanup() throws DBException {
    try {
      // db.close();
      cluster.close();
      String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH:mm:ss").format(new java.util.Date());
      System.out.println("Finish running at "+timeStamp);
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

  @Override
  public Status multiget(String query, Map<String, Map<String, ByteIterator>> result) {
    try {
      System.out.println(query);
      List<Result> results = executeQuery(query);
      return Status.OK;

    } catch (Exception e) {
      // logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status manyget(String query, Map<String, Map<String, ByteIterator>> result) {
    try {
      System.out.println(query);
      List<Result> results = executeQuery(query);
      return Status.OK;


    } catch (Exception e) {
      // logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
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
}

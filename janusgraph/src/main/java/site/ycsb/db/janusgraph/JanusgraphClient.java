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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.script.Bindings;
import javax.script.ScriptException;

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
 * FoundationDB client for YCSB framework.
 */

public class JanusgraphClient extends DB {
  private static final String HOST_NAME_DEFAULT = "nugraphservice-testyimingfdbntypes2-lvs-internal.vip.ebay.com";
  private static final String HOST_NAME = "nugraph.hostname";
  private static final String AUTH_OVERRIDE = "nugraph.authorityoverride";
  private static final String AUTH_OVERRIDE_DEFAULT = "nugraphservice-lvs.monstor-internal.svc.22.tess.io";
  private static final String KEYSPACE = "nugraph.keyspace";
  private static final String KEYSPACE_DEFAULT = "ldbc_sf_01_b";


  // private static AspectGraphParam paramList;
  private RemoteNuGraphTraversalSource g;
  private GremlinGroovyScriptEngine gremlinEngine;
  private Bindings bindings;

  private static Logger logger = LoggerFactory.getLogger(JanusgraphClient.class);

  public RemoteNuGraphTraversalSource getInstance(String hostName, 
      String authOverride, String keyspace) {
    if (g == null) {

      try {
        AbstractNuGraphConfig config = new CustomNuGraphConfig(
            hostName, hostName, true, authOverride
        );
        NuGraphConfigManager.setDefaultConfigAndInit("YCSB", config);

        //create remote graph traversal source
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

  private void process(String line, int repeat) {
    String script = line.split(" \\| ")[0];

    for (int i = 0; i < repeat; ++i) {
      final Object scriptResult;
      GraphTraversal gt = null;
      try {
        scriptResult = gremlinEngine.eval(script, bindings);
        if (scriptResult instanceof GraphTraversal) {
          gt = (GraphTraversal) scriptResult;
          // log.info("To execute graph traversal: {}", GroovyTranslator.of("g").translate(gt.asAdmin().getBytecode()));
          List<?> result = new ArrayList<>();
          gt.fill(result);

          // log.info("Got result (size={}): ", result.size());
          // for (Object obj : result) {
          //     if (obj instanceof Vertex) {
          //         log.info("Vertex id={}", ((Vertex) obj).id());
          //     } else if (obj instanceof Edge) {
          //         log.info("Edge id={}", ((Edge) obj).id());
          //     } else if (obj instanceof Map) {
          //         log.info("Map: ");
          //         Map<?, ?> map = (Map<?, ?>) obj;
          //         for (Map.Entry<?, ?> entry : map.entrySet()) {
          //             log.info("  Key={}, Value={}", entry.getKey(), entry.getValue());
          //         }
          //     } else {
          //         log.info("Got {}", obj);
          //     }
          // }
        } else {
          // log.info("Get script result: {}", scriptResult);
        }
        g.tx().commit();
      } catch (ScriptException e) {
        logger.error("Could not evaluate script {}", script, e);
        g.tx().rollback();
      } catch (Throwable e) {
        logger.error("Got exception", e);
        g.tx().rollback();
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
  }
  @Override
  public void init() throws DBException {
    // initialize FoundationDB driver
    try {
      final Properties props = getProperties();
      String hostname = props.getProperty(HOST_NAME, HOST_NAME_DEFAULT);
      String authOverride = props.getProperty(AUTH_OVERRIDE, AUTH_OVERRIDE_DEFAULT);
      String keyspace = props.getProperty(KEYSPACE, KEYSPACE_DEFAULT);

      System.out.println("hostname: " + hostname);
      System.out.println("auth: " + authOverride);
      System.out.println("keyspace: " + keyspace);

      getInstance(hostname, authOverride, keyspace);

      gremlinEngine = new GremlinGroovyScriptEngine();
      bindings = gremlinEngine.createBindings();
      bindings.put("g", g);

    } catch (Exception e) {
      e.printStackTrace();
    }
    
    // String apiVersion = props.getProperty(API_VERSION, API_VERSION_DEFAULT);
  }

  @Override
  public void cleanup() throws DBException {
    try {
      // db.close();
      g.close();
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
      process(query, 1);
      return Status.OK;

    } catch (Exception e) {
      // logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
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
}

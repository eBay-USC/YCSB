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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.script.Bindings;
import javax.script.ScriptException;


import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
// import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

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
  private static final String CONFIG_PATH           = "gremlin.config";

  private static Logger logger = LoggerFactory.getLogger(FoundationDBClient.class);
  private JanusGraph graph = null;
  private GraphTraversalSource g;
  private GremlinGroovyScriptEngine gremlinEngine;
  private Bindings bindings;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  private static void process(String line, int repeat, GremlinGroovyScriptEngine gremlinEngine,
                                Bindings bindings, GraphTraversalSource g) {
    String script = line.split(" \\| ")[0];

    // logger.info("Executing script: {} {} times", script, repeat);

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
        // log.info("Transaction committed successfully");
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
    final Properties props = getProperties();
    try {
      String janusGraphConfigFile = props.getProperty(CONFIG_PATH, "./config");
      graph = JanusGraphFactory.open(janusGraphConfigFile);
      g = graph.traversal();
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
      if (graph != null) {
        graph.close();
      }
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
      process(query, 1,gremlinEngine, bindings,g);
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
      process(query, 1,gremlinEngine, bindings,g);
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

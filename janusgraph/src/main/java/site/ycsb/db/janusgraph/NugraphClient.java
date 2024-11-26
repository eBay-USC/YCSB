package site.ycsb.db.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.nugraph.client.config.AbstractNuGraphConfig;
import org.nugraph.client.config.NuGraphConfigManager;
import org.nugraph.client.gremlin.driver.remote.NuGraphClientException;
import org.nugraph.client.gremlin.driver.remote.Options;
import org.nugraph.client.gremlin.driver.remote.ReadMode;
import org.nugraph.client.gremlin.process.traversal.dsl.graph.RemoteNuGraphTraversalSource;
import org.nugraph.client.gremlin.structure.RemoteNuGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;

public class NugraphClient extends DB {
  private static final Logger log = LoggerFactory.getLogger(NugraphClient.class);
  private static final String HOST_NAME_DEFAULT = "nugraphservice-ngendrtest6-lvs.monstor-internal.svc.27.tess.io";
  private static final String HOST_NAME = "nugraph.hostname";
  private static final String AUTH_OVERRIDE = "nugraphservice-lvs.monstor-internal.svc.22.tess.io";
  private static final String AUTH_OVERRIDE_DEFAULT = "nugraphservice-lvs.monstor-internal.svc.22.tess.io";
  private static final String KEYSPACE = "nugraph.keyspace";
  private static final String KEYSPACE_DEFAULT = "ldbc_sf_01_b";
  private static final String PARAM_PATH_DEFAULT = "/opt/KG_NuGraph_data_item_titles_use_kg_eval.tsv";
  private static final String PARAM_PATH = "parameter_path";
  private static AspectGraphParam paramList;
  private static RemoteNuGraphTraversalSource g;


  public synchronized static RemoteNuGraphTraversalSource getInstance(String hostName, String authOverride, String keyspace) {
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

 /*
 * For Query one
 * */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    List<String> curParam;
    do {
      curParam = paramList.getRandom();
    } while ("[]".equals(curParam.get(2)));

    String category_id =  curParam.get(1);
    String graphID = String.format("https://site_0_category_%s.aspect.ks.ebay.com",category_id);
    String []entityIDs = curParam.get(2).substring(1, curParam.get(2).length() - 1).replace("'", "").split(",");
    List<GraphTraversal<?, ?>> unions = new ArrayList<>(entityIDs.length);
    for(String entityID : entityIDs){
      entityID = entityID.trim();

      unions.add(
          __.V()
              .has("graph_id",graphID)
              .has("entity_id",entityID.substring(1,entityID.length()-1))
              .limit(1L));

    }
    GraphTraversal<Object, Map<String, Object>> gt = g.union(unions.toArray(new GraphTraversal[0]))
        .local(__.outE("related_aspect")
            .has("probability",P.gt(0.02)).order().by("probability",Order.desc))
        .project("destinationProps","edgeProps","sourceProps")
        .by(__.inV().valueMap()).by(__.valueMap())
        .by(__.outV().valueMap())
        ;

    System.out.println(GroovyTranslator.of("g").translate(gt.asAdmin().getBytecode()).getScript());
    if(log.isDebugEnabled()){
      log.debug(GroovyTranslator.of("g").translate(gt.asAdmin().getBytecode()).getScript());
    }
    return Status.NOT_IMPLEMENTED;
  }


  /*
   * For Query 2
   * */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    List<String> curParam;

    do {
      curParam = paramList.getRandom();
    } while ("[]".equals(curParam.get(2)));


    String category_id =  curParam.get(1);
    String graphID = String.format("https://site_0_category_%s.aspect.ks.ebay.com",category_id);
    String []entityIDs = curParam.get(2).substring(1, curParam.get(2).length() - 1).replace("'", "").split(",");
    List<GraphTraversal<?, ?>> ors = new ArrayList<>(entityIDs.length);
    for(String entityID : entityIDs){
      entityID = entityID.trim();

      ors.add(
          __.V()
              .has("graph_id",graphID)
              .has("entity_id",entityID.substring(1,entityID.length()-1)));
    }
    GraphTraversal<Vertex, Map<String, Object>> gt = g.V().or(ors.toArray(new GraphTraversal[0])).outE("related_aspect")
        .has("probability",0.02)
        .order().by("probability",Order.desc)
        .project("outE","backwardV").by(__.project("sourceProperties","edgeProps","destinationProperties")
            .by(__.outV().elementMap())
            .by(__.elementMap())
            .by(__.inV().elementMap()))
        .by(__.inV().outE("related_aspect").has("probability",0.02)
            .order().by("probability",Order.desc).dedup().inV().id().fold());
    //System.out.println(GroovyTranslator.of("g").translate(gt.asAdmin().getBytecode()).getScript());
    if(log.isDebugEnabled()){
     log.debug(GroovyTranslator.of("g").translate(gt.asAdmin().getBytecode()).getScript());
   }
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
  @Override
  public Status delete(String table, String key) {

    return Status.NOT_IMPLEMENTED;
  }



  @Override
  public void init() throws DBException {
    try {
      final Properties props = getProperties();
      String hostname = props.getProperty(HOST_NAME,HOST_NAME_DEFAULT);
      String authOverride = props.getProperty(AUTH_OVERRIDE, AUTH_OVERRIDE_DEFAULT);
      String keyspace = props.getProperty(KEYSPACE,KEYSPACE_DEFAULT);
      String paramPath = props.getProperty(PARAM_PATH,PARAM_PATH_DEFAULT);
      System.out.println("hostname: " + hostname);
      System.out.println("auth: " + authOverride);
      System.out.println("keyspace: " + keyspace);
      System.out.println("paramPath: " + paramPath);
      getInstance(hostname, authOverride, keyspace);
      paramList= new AspectGraphParam(paramPath);
      System.out.println(g.V().has("person", "id", 37383395344651L).next());
    } catch (Exception e) {
      throw new DBException(e);
    }

  }
}

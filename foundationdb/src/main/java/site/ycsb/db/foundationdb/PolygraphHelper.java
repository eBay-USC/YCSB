package site.ycsb.db.foundationdb;
import java.util.ArrayList;
// import java.util.Properties;

/**
 * Represents an entity in the FoundationDB database.
 * This class contains fields such as key, name, and properties.
 */
public final class PolygraphHelper {

  // public static KafkaProducer<String, String> kafkaProducer = null;
  // public static int tid = 0;
  private PolygraphHelper() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated.");
  }
  public static final char NO_READ_UPDATE = 'X';
  public static final char KEY_SEPERATOR = '-';
  public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
  public static final char ENTITY_SEPERATOR = '&';
  public static final char PROPERY_SEPERATOR = '#';
  public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
  public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
  public static final String ESCAPE_START_CHAR = "\\(";
  public static final String ESCAPE_END_CHAR = "\\)";

  public static final int KEY_SEPERATOR_NUM = 1;
  public static final int RECORD_ATTRIBUTE_SEPERATOR_NUM = 2;
  public static final int ENTITY_SEPERATOR_NUM = 3;
  public static final int PROPERY_SEPERATOR_NUM = 4;
  public static final int PROPERY_ATTRIBUTE_SEPERATOR_NUM = 5;
  public static final int ENTITY_ATTRIBUTE_SEPERATOR_NUM = 6;
  public static final int ESCAPE_START_NUM = 7;
  public static final int ESCAPE_END_NUM = 8;
  // public static final int numPartitions_AppName_1 = 4;

  public static int convertStringToInt(String str) {
    int result = 0;
    for (int i = 0; i < str.length(); i++) {
      result += str.charAt(i);
    }
    return result;
  }

  public static int getPartitionKey(int[] pPAs) {
    int result = 0;
    for (int i = 0; i < pPAs.length; i++) {
      result += pPAs[i] * Math.pow(10, pPAs.length - 1 - i);
    }
    return result;
  }

  public static String concatWithSeperator(char seperator, String... params) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < params.length; i++) {
      sb.append(params[i]);
      if (i + 1 != params.length) {
        sb.append(seperator);
      }
    }
    return sb.toString();
  }

  public static String getLogRecordString(char type, String actionName, 
      String recordKey, long startTime, long endTime, ArrayList<Entity> entitiesArr) {
    StringBuilder sb = new StringBuilder();
    String res = concatWithSeperator(RECORD_ATTRIBUTE_SEPERATOR, String.valueOf(type), 
        actionName, recordKey, String.valueOf(startTime), String.valueOf(endTime), generateEntitiesString(entitiesArr));
    return res;
  }

  public static String generateEntitiesString(ArrayList<Entity> entities) {
    StringBuilder sb = new StringBuilder();
    try {

      String eSeperator = "";
      for (Entity e : entities) {
        if (e.getProperties().length < 1) {
          continue;
        }
        sb.append(eSeperator);
        sb.append(concatWithSeperator(ENTITY_ATTRIBUTE_SEPERATOR, e.name, e.key));
        sb.append(ENTITY_ATTRIBUTE_SEPERATOR);
        String pSeperator = "";
        for (Propertyy p : e.getProperties()) {
          sb.append(pSeperator);
          sb.append(concatWithSeperator(PROPERY_ATTRIBUTE_SEPERATOR, 
              p.getName(), p.getValue(), String.valueOf(p.getType())));
          pSeperator = String.valueOf(PROPERY_SEPERATOR);
        }
        eSeperator = String.valueOf(ENTITY_SEPERATOR);
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace(System.out);
    }

    return sb.toString();
  }


  public static String escapeCharacters(String input) {
    input = input.replaceAll(String.valueOf(ESCAPE_START_CHAR), 
        ESCAPE_START_CHAR + ESCAPE_START_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll("(?<!\\(\\d\\d?)" + String.valueOf(ESCAPE_END_CHAR), 
        ESCAPE_START_CHAR + ESCAPE_END_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll(String.valueOf(KEY_SEPERATOR), 
        ESCAPE_START_CHAR + KEY_SEPERATOR_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll(String.valueOf(RECORD_ATTRIBUTE_SEPERATOR), 
        ESCAPE_START_CHAR + RECORD_ATTRIBUTE_SEPERATOR_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll(String.valueOf(ENTITY_SEPERATOR), 
        ESCAPE_START_CHAR + ENTITY_SEPERATOR_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll(String.valueOf(PROPERY_SEPERATOR), 
        ESCAPE_START_CHAR + PROPERY_SEPERATOR_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll(String.valueOf(PROPERY_ATTRIBUTE_SEPERATOR), 
        ESCAPE_START_CHAR + PROPERY_ATTRIBUTE_SEPERATOR_NUM + ESCAPE_END_CHAR);
    input = input.replaceAll(String.valueOf(ENTITY_ATTRIBUTE_SEPERATOR), 
        ESCAPE_START_CHAR + ENTITY_ATTRIBUTE_SEPERATOR_NUM + ESCAPE_END_CHAR);

    return input;
  }
}


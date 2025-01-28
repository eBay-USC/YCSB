package site.ycsb.db.foundationdb;
/**
 * Represents an entity in the FoundationDB database.
 * This class contains fields such as key, name, and properties.
 */
public class Propertyy {
  private String name;
  private String value;
  private char type = PolygraphHelper.NO_READ_UPDATE;

  public Propertyy(String name, String value, char type) {
    this.name = name;
    this.value = value;
    this.type = type;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public String getValue() {
    return this.value;
  }

  public void setValue(String newValue) {
    this.value = newValue;
  }

  public char getType() {
    return this.type;
  }

  public void setType(char newType) {
    this.type = newType;
  }

  public Propertyy getCopy() {
    Propertyy p = new Propertyy(name, value, type);
    return p;
  }

  public static String getProprtyKey(Entity e, Propertyy p) {
    StringBuilder sb = new StringBuilder();
    sb.append(e.getName());
    sb.append(PolygraphHelper.KEY_SEPERATOR);
    sb.append(e.getKey());
    sb.append(PolygraphHelper.KEY_SEPERATOR);
    sb.append(p.getName());
    return sb.toString();
  }
}

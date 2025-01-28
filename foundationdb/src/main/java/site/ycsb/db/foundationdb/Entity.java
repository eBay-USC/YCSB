package site.ycsb.db.foundationdb;
/**
 * Represents an entity in the FoundationDB database.
 * This class contains fields such as key, name, and properties.
 */
public class Entity {
  protected String key;
  protected String name;
  protected Propertyy[] properties;

  public Entity(String key, String name, Propertyy[] properties) {
    this.key = key;
    this.name = name;
    this.properties = properties;
  }

  public String getKey() {
    return this.key;
  }

  public void setKey(String newKey) {
    this.key = newKey;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public Propertyy[] getProperties() {
    return this.properties;
  }

  public void setProperties(Propertyy[] newProperties) {
    this.properties = newProperties;
  }

  public Entity getCopy() {
    Propertyy[] newPA = new Propertyy[this.properties.length];
    for (int i = 0; i < properties.length; i++) {
      newPA[i] = properties[i].getCopy();
    }
    Entity result = new Entity(key, name, newPA);
    return result;
  }

  public boolean same(Entity e) {
    if (properties.length != e.properties.length) {
      return false;
    }
    for (int i = 0; i < properties.length; i++) {
      if (!properties[i].getValue().equals(e.properties[i].getValue())) {
        return false;
      }
    }
    return true;
  }

  public String getEntityKey() {
    return PolygraphHelper.concatWithSeperator(PolygraphHelper.KEY_SEPERATOR, name, key);
  }
}

package site.ycsb.db.janusgraph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AspectGraphParam{

  private final List<List<String>> params;
  private Random random;
  public AspectGraphParam(String filePath) {
    random = new Random();
    params = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line = br.readLine();
      while ((line = br.readLine()) != null) {
          String[]s = line.split("\t");
          if(s[3].equals("[]"))continue;
          List<String>elem =new ArrayList<>();
          elem.add(s[0]);
          elem.add(s[2]);
          elem.add(s[3]);
          params.add(elem);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
  public List<String> getRandom() {
    return this.params.get(random.nextInt(params.size()));
  }

}

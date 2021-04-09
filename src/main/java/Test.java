import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class Test {
    public static void main(String[] args){
        //Creating a JSONParser object
        JSONParser jsonParser = new JSONParser();
        try {
            //Parsing the contents of the JSON file
            JSONArray jsonArray = (JSONArray) jsonParser.parse(new FileReader("/Users/eloymarinciudad/Documents/MIRI/1B/BDM/LAB1/BDM_Project1/target/classes/idealista/2020_01_02_idealista.json"));
            Iterator<JSONObject> iterator = jsonArray.iterator();
            while(iterator.hasNext()) {
                System.out.println(iterator.next().get("propertyCode"));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}

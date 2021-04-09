import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class IdealistaToMongoDB {
    private final MongoClient client = new MongoClient("10.4.41.147");
    // TODO: Create database and collection in the VM
    private final MongoDatabase database = client.getDatabase("bdm_project1");
    private final MongoCollection<Document> idealistaCollection = database.getCollection("idealista");

    private List<String> getFilesPaths(String path) throws IOException {
        List<String> filesList = new ArrayList<>();

        final File jarFile = new File(IdealistaToMongoDB.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        final JarFile jar = new JarFile(jarFile);
        final Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
        while (entries.hasMoreElements()) {
            final String name = entries.nextElement().getName();
            if (name.startsWith(path + "/")) { //filter according to the path
                filesList.add(name);
            }
        }
        jar.close();

        return filesList;
    }

    private void parseAndInsert(String filePath){
        JSONParser jsonParser = new JSONParser();
        try {
            //Parsing the contents of the JSON file
            JSONArray jsonArray = (JSONArray) jsonParser.parse(new FileReader(filePath));
            Iterator<JSONObject> iterator = jsonArray.iterator();
            while(iterator.hasNext()) {
                // TODO: Decide what to do once we have each JSONObject
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


    private void idealistaToDB() throws IOException {
        List<String> filesPaths = getFilesPaths("idealista");
        for(String path: filesPaths){
            parseAndInsert(path);
        }
    }


}

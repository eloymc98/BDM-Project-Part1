package idealista;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import utils.PathFinder;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

public class IdealistaToMongoDB {
    private final MongoClient client = new MongoClient("10.4.41.147");
    private final MongoDatabase database = client.getDatabase("bdm2");
    private final MongoCollection<Document> idealistaCollection = database.getCollection("idealista");
    private final Pattern datePattern = Pattern.compile("(19|20)\\d\\d_(0[1-9]|1[012])_(0[1-9]|[12][0-9]|3[01])");
    private final PathFinder finder = new PathFinder();


    private void parseAndInsert(String filePath) {
        String fileDate = null;

        Matcher m = datePattern.matcher(filePath);
        if (m.find()) {
            fileDate = m.group().replace('_', '-');
        }

        JSONParser jsonParser = new JSONParser();
        try {
            //Parsing the contents of the JSON file
            JSONArray jsonArray = (JSONArray) jsonParser.parse(new FileReader(filePath));
            Iterator<JSONObject> iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                jsonToMongoDB(iterator.next(), fileDate);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    private void jsonToMongoDB(JSONObject jsonObject, String date) {
        Document propertyDocument = new Document();
        propertyDocument.put("_id", jsonObject.get("propertyCode"));

        if (idealistaCollection.countDocuments(propertyDocument) == 0) {
            propertyDocument.put("size", jsonObject.get("size"));
            propertyDocument.put("rooms", jsonObject.get("rooms"));
            propertyDocument.put("bathrooms", jsonObject.get("bathrooms"));
            //propertyDocument.put("country", jsonObject.get("country"));
            //propertyDocument.put("province", jsonObject.get("province"));
            //propertyDocument.put("municipality", jsonObject.get("municipality"));
            propertyDocument.put("district", jsonObject.get("district"));
            propertyDocument.put("neighborhood", jsonObject.get("neighborhood"));

            Document priceDocument = new Document();
            priceDocument.put("date", date);
            priceDocument.put("price", jsonObject.get("price"));
            //priceDocument.put("priceByArea", jsonObject.get("priceByArea"));

            List<Document> prices = new ArrayList<>();
            prices.add(priceDocument);

            propertyDocument.put("prices", prices);
            idealistaCollection.insertOne(propertyDocument);
        } else {
            // TODO: Check if exists data from current date
            Document newDatePrice = new Document();
            newDatePrice.put("date", date);
            newDatePrice.put("price", jsonObject.get("price"));
            newDatePrice.put("priceByArea", jsonObject.get("priceByArea"));
            idealistaCollection.updateOne(eq("_id", jsonObject.get("propertyCode")),
                    Updates.addToSet("prices", newDatePrice));
        }

    }


    public void idealistaToDB() throws IOException {
        List<String> filesPaths = finder.getFilesPaths("idealista");
        for (String path : filesPaths) {
            parseAndInsert(path);
        }
        System.out.println("Data import succeeded!");
        client.close();
    }


}
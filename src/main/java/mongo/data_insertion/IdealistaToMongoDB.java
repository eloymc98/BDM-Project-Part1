package mongo.data_insertion;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import utils.Utils;

import javax.print.Doc;
import java.io.FileReader;
import java.io.IOException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;

public class IdealistaToMongoDB {
    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> auxiliaryStructuresCollection;
    private final Pattern datePattern = Pattern.compile("(19|20)\\d\\d_(0[1-9]|1[012])_(0[1-9]|[12][0-9]|3[01])");

    public IdealistaToMongoDB(MongoClient client, String database) {
        this.collection = client.getDatabase(database).getCollection("idealista");
        this.auxiliaryStructuresCollection = client.getDatabase(database).getCollection("auxiliary_structures");
    }

    public void idealistaToDB() throws IOException {
        List<String> filesPaths = Utils.getFilesPaths("idealista");
        for (String path : filesPaths) {
            parseAndInsert(path);
        }
        System.out.println("Idealista data import succeeded!");
    }

    private void parseAndInsert(String filePath) {
        String fileDate = null;

        // Get date from filename
        Matcher m = datePattern.matcher(filePath);
        if (m.find()) {
            fileDate = m.group().replace('_', '-');
        }

        JSONParser jsonParser = new JSONParser();
        try {
            Set<String> propertiesSet = new HashSet<>();
            Map<String, Set<String>> neighborhoodProperties = new HashMap<>();
            Map<String, Set<String>> districtProperties = new HashMap<>();


            //Parsing the contents of the JSON file
            JSONArray jsonArray = (JSONArray) jsonParser.parse(new FileReader(filePath));
            Iterator<JSONObject> iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = iterator.next();
                String propertyId = (String) jsonObject.get("propertyCode");

                // We check if a property appears more than once in the same day...
                // In case of a duplicate property, we do not insert its data twice
                if (!propertiesSet.contains(propertyId)) {
                    propertiesSet.add(propertyId);
                    jsonToMongoDB(jsonObject, fileDate);
                }

                // Keep track of properties that appear in a neighborhood
                String neighborhood = (String) jsonObject.get("neighborhood");

                if (neighborhood != null) {
                    if (!neighborhoodProperties.containsKey(neighborhood)) {
                        Set<String> set = new HashSet<>();
                        set.add(propertyId);
                        neighborhoodProperties.put(neighborhood, set);
                    } else {
                        Set<String> set = neighborhoodProperties.get(neighborhood);
                        set.add(propertyId);
                        neighborhoodProperties.put(neighborhood, set);
                    }
                }


                //Keep track of properties that appear in a district
                String district = (String) jsonObject.get("district");
                if (district!=null){
                    if (!districtProperties.containsKey(district)) {
                        Set<String> set = new HashSet<>();
                        set.add(propertyId);
                        districtProperties.put(district, set);
                    } else {
                        Set<String> set = districtProperties.get(district);
                        set.add(propertyId);
                        districtProperties.put(district, set);
                    }
                }


            }

            // Create/update auxiliar structures
            neighborhoodProperties.forEach((k,v)-> updateAuxiliarDocument(k,v,"neighborhood"));
            districtProperties.forEach((k,v)-> updateAuxiliarDocument(k,v,"district"));
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }


    private void jsonToMongoDB(JSONObject jsonObject, String date) {
        // Insert property price data
        Document propertyDocument = new Document();
        propertyDocument.put("_id", jsonObject.get("propertyCode"));
        if (collection.countDocuments(propertyDocument) == 0) {
            propertyDocument.put("size", jsonObject.get("size"));
            propertyDocument.put("rooms", jsonObject.get("rooms"));
            propertyDocument.put("bathrooms", jsonObject.get("bathrooms"));
            propertyDocument.put("municipality", jsonObject.get("municipality"));
            propertyDocument.put("district", jsonObject.get("district"));
            propertyDocument.put("neighborhood", jsonObject.get("neighborhood"));

            List<Object> prices = new ArrayList<>();
            prices.add(jsonObject.get("price"));
            propertyDocument.put("prices", prices);

            List<String> dates = new ArrayList<>();
            dates.add(date);
            propertyDocument.put("dates", dates);
            collection.insertOne(propertyDocument);
        } else {
            // If the property document already exists, add new price and date data
            collection.updateOne(eq("_id", jsonObject.get("propertyCode")),
                    Updates.push("prices", jsonObject.get("price")));
            collection.updateOne(eq("_id", jsonObject.get("propertyCode")),
                    Updates.push("dates", date));
        }

    }

    private void updateAuxiliarDocument(String key, Set<String> properties, String type) {
        Document aux = new Document();
        List<String> propertiesList = new ArrayList<>(properties);

        aux.put("_id", key + "-" + type);
        if (auxiliaryStructuresCollection.countDocuments(aux) == 0) {
            aux.put("properties", propertiesList);
            auxiliaryStructuresCollection.insertOne(aux);
        } else {
            auxiliaryStructuresCollection.updateOne(aux,
                    Updates.addEachToSet("properties", propertiesList));
        }
    }

}

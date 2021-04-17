package mongo.data_insertion;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.opencsv.CSVReader;
import org.bson.Document;

import utils.Utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LookupTablesToMongoDB {
    private final MongoCollection<Document> collection;

    public LookupTablesToMongoDB(MongoClient client, String database){
        this.collection = client.getDatabase(database).getCollection("lookup_tables");
    }

    public void lookupTablesToDB() throws IOException {
        List<String> filesPaths = Utils.getFilesPaths("lookup_tables");
        parseAndInsert(filesPaths);
        System.out.println("Lookup tables import succeeded!");
    }

    private void parseAndInsert(List<String> filePaths) throws IOException {
        Map<String, Map<String,String>> lookupTablesMap = new HashMap<>();

        // We merge (full-join style) both lookup tables
        for (String file: filePaths){
            CSVReader reader = new CSVReader(new FileReader(file));
            String name;
            if (file.contains("idealista_extended.csv")){
                name = "idealista";
            }else{
                name = "income";
            }
            // Skip header
            reader.readNext();

            String[] line;
            Iterator it = reader.iterator();
            while(it.hasNext()) {
                line = (String[]) it.next();

                if (!lookupTablesMap.containsKey(line[7])){
                    Map<String,String> value = new HashMap<>();
                    value.put("districtId", line[4]);
                    value.put(name + "_neighborhood", line[1]);
                    value.put(name + "_district", line[0]);
                    lookupTablesMap.put(line[7], value);
                }else{
                    Map<String,String> value = lookupTablesMap.get(line[7]);
                    value.put(name + "_neighborhood", line[1]);
                    value.put(name + "_district", line[0]);
                    lookupTablesMap.put(line[7], value);
                }

            }
        }

        List<Document> documents = new ArrayList<>();
        lookupTablesMap.forEach((k,v) -> documents.add(parseToDocument(k,v)));
        collection.insertMany(documents);
    }

    private Document parseToDocument(String neighborhoodId, Map<String,String> neighborhoodMap){
        Document neighborhood = new Document();
        // Use as document id the neighborhood ID
        neighborhood.put("_id", neighborhoodId);
        if (collection.countDocuments(neighborhood) == 0){
            neighborhoodMap.forEach((k,v) -> neighborhood.put(k,v));
        }
        return neighborhood;
    }


}

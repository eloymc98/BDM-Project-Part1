package mongo.data_insertion;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.opencsv.CSVReader;
import org.bson.Document;
import utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.FileReader;
import java.util.Iterator;

public class BCNIncomeToMongoDB {
    private final MongoCollection<Document> collection;

    public BCNIncomeToMongoDB(MongoClient client, String database){
        this.collection = client.getDatabase(database).getCollection("bcn_income");
    }

    public void incomeToDB() throws IOException {
        List<String> filesPaths = Utils.getFilesPaths("opendatabcn-income");

        for (String path : filesPaths) {
            parseAndInsert(path);
        }
        System.out.println("BCN income data import succeeded!");
    }

    private void parseAndInsert(String filePath) throws IOException {
        //Instantiating the CSVReader class
        CSVReader reader = new CSVReader(new FileReader(filePath));
        // Skip header
        reader.readNext();
        String[] line;
        //Getting the Iterator object
        Iterator it = reader.iterator();
        while(it.hasNext()) {
            line = (String[]) it.next();
            // Filter out missing data
            if (!line[2].equals("No consta")){
                CSVToDocument(line);
            }
        }
    }

    private void CSVToDocument(String[] line){
        Document neighborhoodRFDDocument = new Document();

        // We use as _id the year and the neighborhood code
        neighborhoodRFDDocument.put("_id", line[0] + line[3]);
        if (collection.countDocuments(neighborhoodRFDDocument) == 0){
            neighborhoodRFDDocument.put("year", Integer.parseInt(line[0]));
            neighborhoodRFDDocument.put("district", line[2]);
            neighborhoodRFDDocument.put("neighborhood", line[4]);
            neighborhoodRFDDocument.put("population", Integer.parseInt(line[5]));
            neighborhoodRFDDocument.put("rfdIndex", Float.parseFloat(line[6]));
            collection.insertOne(neighborhoodRFDDocument);
        }
    }
}

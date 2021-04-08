import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class IdealistaToMongoDB {
    public void insertData(){
        MongoClient client = new MongoClient("10.4.41.147");
        // TODO: Create database and collection in the VM
        MongoDatabase database = client.getDatabase("bdm_project1");
        MongoCollection<Document> idealistaCollection = database.getCollection("idealista");

        // TODO: Loop over all idealista files, parse and insert them

    }
}

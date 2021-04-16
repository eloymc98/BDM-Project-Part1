import com.mongodb.MongoClient;
import mongo.data_insertion.BCNIncomeToMongoDB;
import mongo.data_insertion.IdealistaToMongoDB;
import mongo.data_insertion.LookupTablesToMongoDB;

public class Main {
    public static void main(String[] args){
        MongoClient client = new MongoClient("10.4.41.147");
        BCNIncomeToMongoDB income = new BCNIncomeToMongoDB(client, "bdm_p1");
        IdealistaToMongoDB idealista = new IdealistaToMongoDB(client, "bdm_p1");
        LookupTablesToMongoDB lookup = new LookupTablesToMongoDB(client, "bdm_p1");
        try {
            income.incomeToDB();
            lookup.lookupTablesToDB();
            idealista.idealistaToDB();
            client.close();
        }catch (java.io.IOException e){
            e.printStackTrace();
        }
    }

}

package pl.jb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class Traditional {

    public static void main(String[] args) {
        String connectionString = "mongodb+srv://myAtlasDBUser:myatlas-001@myatlasclusteredu.u5mgn.mongodb.net/?retryWrites=true&w=majority&appName=myAtlasClusterEDU";
        MongoClient mongoClient = null;
        try {
            mongoClient = MongoClients.create(connectionString);

            List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
            databases.forEach(db -> System.out.println(db.toJson()));

            MongoDatabase db = mongoClient.getDatabase("sample_analytins");
            System.out.println("Database name: " + db.getName());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {

            if(mongoClient != null) {
                mongoClient.close();
            }
        }
    }
}

package pl.jb.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

public class Crud {
    public static void main(String[] args) {
        String connectionString = "mongodb+srv://myAtlasDBUser:myatlas-001@myatlasclusteredu.u5mgn.mongodb.net/?retryWrites=true&w=majority&appName=myAtlasClusterEDU";
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applicationName("akuku")
                .readConcern(ReadConcern.MAJORITY)
                .build();

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
            databases.forEach(db -> System.out.println(db.toJson()));

            MongoDatabase database = mongoClient.getDatabase("bank");
            MongoCollection<Document> collection = database.getCollection("accounts");
            // Find
            try(MongoCursor<Document> cursor = collection.find(and(gte("balance", 1000),eq("account_type","checking")))
                    .iterator())
            {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            }

            //Find first
            Document json = collection.find(and(gte("balance", 1000), eq("account_type", "checking"))).first();
            System.out.println(json.toJson());

            // UpdateOne
            try {
                Bson query = Filters.eq("account_id", "MDB12234728");
                Bson updates = Updates.combine(Updates.set("account_status", "active"), Updates.inc("balance", 100));
                UpdateResult upResult = collection.updateOne(query, updates);
                System.out.println(upResult.getModifiedCount());
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            //UpdateMany
            try {
                Bson query = Filters.eq("account_type", "savings");
                Bson updates = Updates.combine(Updates.set("minimum_balance", 100));
                UpdateResult upResult = collection.updateMany(query, updates);
                System.out.println("Update result modified count: " + upResult.getModifiedCount());
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            // Delete one
            try {
                Bson query = Filters.eq("account_holder", "john doe");
                DeleteResult delResult = collection.deleteOne(query);
                System.out.println("Deleted a document:");
                System.out.println("\t" + delResult.getDeletedCount());
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            // Delete many
            try {
                Bson query = eq("account_status", "dormant");
                DeleteResult delResult = collection.deleteMany(query);
                System.out.println(delResult.getDeletedCount());
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }


    }
}
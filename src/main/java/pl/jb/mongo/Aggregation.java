package pl.jb.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Sorts.*;
import static java.util.Arrays.asList;

public class Aggregation {

    public static void main(String[] args) {
        String connectionString = System.getProperty("mongodb.uri");
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase("bank");
            MongoCollection<Document> accounts = db.getCollection("accounts");
            matchAndGroupStages(accounts);
            matchSortAndProjectStages(accounts);
        }
    }

    private static void matchSortAndProjectStages(MongoCollection<Document> accounts) {
        Bson matchStage =
                Aggregates.match(Filters.and(Filters.gt("balance", 1500), Filters.eq("account_type", "checking")));
        Bson sortStage = Aggregates.sort(Sorts.orderBy(descending("balance")));
        Bson projectStage = Aggregates.project(Projections.fields(Projections.include("account_id", "account_type", "balance"), Projections.computed("euro_balance", new Document("$divide", asList("$balance", 1.20F))), Projections.excludeId()));
        System.out.println("Display aggregation results");
    }

    private static void matchAndGroupStages(MongoCollection<Document> accounts) {
//        Bson matchStage = Aggregates.match(Filters.eq("account_id", "MDB310054629"));
        Bson matchStage = Aggregates.match(Filters.lt("balance", 1000));
        Bson groupStage = Aggregates.group("$account_type", sum("total_balance", "$balance"), avg("average_balance", "$balance"));
        System.out.println("Display aggregation results");
        accounts.aggregate(asList(matchStage, groupStage)).forEach(document->System.out.print(document.toJson()));
    }
}

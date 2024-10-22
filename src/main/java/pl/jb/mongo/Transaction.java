package pl.jb.mongo;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.eq;


public class Transaction {



    public static void main(String[] args) {
        final MongoClient client = MongoClients.create("mongodb://localhost:27017/imdas");
        final ClientSession clientSession = client.startSession();
        TransactionBody txnBody = (TransactionBody<String>) () -> {

            MongoCollection<Document> bankingCollection = client.getDatabase("bank").getCollection("accounts");

            Bson fromAccount = eq("account_id", "MDB310054629");
            Bson withdrawal = Updates.inc("balance", -200);

            Bson toAccount = eq("account_id", "MDB643731035");
            Bson deposit = Updates.inc("balance", 200);

            System.out.println("This is from Account " + fromAccount.toBsonDocument().toJson() + " withdrawn " + withdrawal.toBsonDocument().toJson());
            System.out.println("This is to Account " + toAccount.toBsonDocument().toJson() + " deposited " + deposit.toBsonDocument().toJson());
            bankingCollection.updateOne(clientSession, fromAccount, withdrawal);
            bankingCollection.updateOne(clientSession, toAccount, deposit);

            return "Transferred funds from John Doe to Mary Doe";
        };

        try {
            clientSession.withTransaction(txnBody);
        } catch (RuntimeException e){
            System.out.println(e);
        }finally{
            clientSession.close();
        }
    }
}

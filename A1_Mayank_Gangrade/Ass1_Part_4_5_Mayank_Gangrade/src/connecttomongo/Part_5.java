/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package connecttomongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.bson.Document;

/**
 *
 * @author gangrade.m
 */
public class Part_5 {

    public static void main(String[] args) {

//      Connecting with MonfgoDb
        MongoClient connection = MongoClients.create();

//      Creating database
        MongoDatabase db = connection.getDatabase("logdb");

//      Creating collection
        MongoCollection<Document> collection = db.getCollection("log");

//      Creating list of dcoument
        List<Document> documents = new ArrayList<Document>();

        try {
            Scanner scanner = new Scanner(new File("C:\\Users\\gangrade.m\\Documents\\NetBeansProjects\\ConnectToMongo\\src\\connecttomongo\\accesslog.txt"));
            while (scanner.hasNextLine()) {
                String[] words = scanner.nextLine().split("\\s");
                System.out.println(scanner.nextLine());

                String ip_address = words[0];
                String address = words[6];
                String date = words[3].substring(1, 3);
                String month = words[3].substring(4, 7);
                String year = words[3].substring(8, 12);

                System.out.println(ip_address + address + date + month + year);

                Document doc = new Document();
                doc.append("ip_address", ip_address);
                doc.append("address", address);
                doc.append("date", date);
                doc.append("month", month);
                doc.append("year", year);

                documents.add(doc);
//                collection.insertOne(doc);
            }
            collection.insertMany(documents);
            scanner.close();
        } catch (FileNotFoundException e) {

        }
    }
}

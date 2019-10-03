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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import org.bson.Document;

/**
 *
 * @author gangrade.m
 */
public class Part_4 {

    public static void uplodMovies(MongoCollection<Document> movie_collection) {
        System.out.println("uploading movies data start");
        BufferedReader movie_reader;
        try {
            movie_reader = new BufferedReader(new FileReader("C:\\Users\\gangrade.m\\Documents\\NetBeansProjects\\ConnectToMongo\\src\\connecttomongo\\ml-10m\\movies.dat"));
            String line = movie_reader.readLine();
            while (line != null) {
//                System.out.println(line);
                // read next line
                String[] words = line.split("::");

                Document doc = new Document();
                doc.append("movieId", words[0]);
                doc.append("title", words[1].substring(0, words[1].length() - 7));
                doc.append("year", words[1].substring(words[1].length() - 5, words[1].length() - 1));
                doc.append("genres", Arrays.asList(words[2].split("\\|")));

                movie_collection.insertOne(doc);
                line = movie_reader.readLine();
            }
            movie_reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void uploadTags(MongoCollection<Document> tag_collection) {
        System.out.println("upload tag data start");
        BufferedReader tag_reader;
        try {
            tag_reader = new BufferedReader(new FileReader("C:\\Users\\gangrade.m\\Documents\\NetBeansProjects\\ConnectToMongo\\src\\connecttomongo\\ml-10m\\tags.dat"));
            String line3 = tag_reader.readLine();
            while (line3 != null) {
//                System.out.println(line3);
                // read next line
                String[] words = line3.split("::");

                Document doc = new Document();
                doc.append("userId", words[0]);
                doc.append("movieId", words[1]);
                doc.append("tag", words[2]);
                doc.append("timeStamp", words[3]);

                tag_collection.insertOne(doc);
                line3 = tag_reader.readLine();
            }
            tag_reader.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public static void uploadRating(MongoCollection<Document> rating_collection) {
        System.out.println("upload rating data start");
        BufferedReader rating_reader;
        try {
            rating_reader = new BufferedReader(new FileReader("C:\\Users\\gangrade.m\\Documents\\NetBeansProjects\\ConnectToMongo\\src\\connecttomongo\\ml-10m\\ratings.dat"));
            String line2 = rating_reader.readLine();
            while (line2 != null) {
                System.out.println(line2);
                // read next line
                String[] words = line2.split("::");

                Document doc = new Document();
                doc.append("userId", words[0]);
                doc.append("movieId", words[1]);
                doc.append("rating", words[2]);
                doc.append("timeStamp", words[3]);

                rating_collection.insertOne(doc);
                line2 = rating_reader.readLine();
            }
            rating_reader.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public static void main(String[] args) {
//      Connecting with MonfgoDb
        MongoClient connection = MongoClients.create();

//      Creating database
        MongoDatabase db = connection.getDatabase("moviedb");

//      Creating collection
        MongoCollection<Document> movie_collection = db.getCollection("movie");
        MongoCollection<Document> rating_collection = db.getCollection("rating");
        MongoCollection<Document> tag_collection = db.getCollection("tag");

        uplodMovies(movie_collection);
        uploadTags(tag_collection);
        uploadRating(rating_collection);
    }

}

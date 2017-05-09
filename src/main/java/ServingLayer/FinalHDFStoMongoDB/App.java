package ServingLayer.FinalHDFStoMongoDB;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;


import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONObject;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;



/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ){
    	
        String pathToHFile="/Users/Shreya/Desktop/hdfs-output.txt";
        String pathToKafkaFile="/Users/Shreya/Desktop/kafka-output.txt";

    	// connect to mongoDB, IP and port number
        MongoClient client = new MongoClient("localhost", 27017);
        
        // get database from MongoDB,
        MongoDatabase db = client.getDatabase("AdAnalyticsPlatform");
    	 
        // get a single collection
        MongoCollection<Document> newColl = db.getCollection("users");
    	 
        // open file
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream(pathToKafkaFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("file not exist, exiting");
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
        String str="",useremail="",productCategory,demographicsAgeMax,demographicsAgeMin,demographicsGender,budget,demographicsCity;
        // read it line by line
        String strLine;
        //Document opDocument = null;
        JSONObject outputDoc=new JSONObject();		
        
        try {
            while ((strLine = br.readLine()) != null) {
                str= str + strLine;
            }

            System.out.println("KAFKA FILE CONTENT "+str);
            // convert line by line to BSON
           
            DBObject bson = (DBObject) JSON.parse(str);
            // insert BSONs to database
            try {
                
                useremail=bson.get("email").toString();
                productCategory=bson.get("product_category").toString();
                demographicsAgeMax=bson.get("age_max").toString();
                demographicsAgeMin=bson.get("age_min").toString();
                demographicsGender=bson.get("gender").toString();
                budget=bson.get("budget").toString();
                demographicsCity=bson.get("city").toString();
                

                //Populate the final JSON Object to be inserted in the MongoDB Collection
                outputDoc.put("productCategory", productCategory);
                outputDoc.put("demographicsAgeMax", demographicsAgeMax);
                outputDoc.put("demographicsAgeMin", demographicsAgeMin);
                outputDoc.put("demographicsGender", demographicsGender);
                outputDoc.put("budget", budget);
                outputDoc.put("demographicsCity", demographicsCity);
                
                
            }
            catch (MongoException e) {
              e.printStackTrace();
          }

          br.close();
      } catch (IOException e) {
        e.printStackTrace();
    }

       System.out.println("Output Document "+outputDoc);
    
        
     // open file
    FileInputStream fileStream = null;
    try {
    	fileStream = new FileInputStream(pathToHFile);
    } catch (FileNotFoundException e) {
        e.printStackTrace();
        System.out.println("file not exist, exiting");
    }

    BufferedReader brdr = new BufferedReader(new InputStreamReader(fileStream));
    String s="";
    // read it line by line
    String sLine;


    try {
        while ((sLine = brdr.readLine()) != null) {
         s= s+sLine + "\n";
        }

     s=s.replace('\t', ':');
     s=s.replace('\n', ',');
     s = "{" + s + "}";
     System.out.println(s);
     
     outputDoc.put("platformID", s);
     System.out.println("FINAL OP "+outputDoc);
     System.out.println("EMAIL "+useremail);
     
    
    
		Bson filter = new Document("email", useremail);
		Bson newValue = new Document("adPlan", outputDoc);
		Bson updateOperationDocument = new Document("$push", newValue);
		newColl.updateOne(filter, updateOperationDocument);

    brdr.close();
    } catch (IOException e) {
        e.printStackTrace();
    }

}
}

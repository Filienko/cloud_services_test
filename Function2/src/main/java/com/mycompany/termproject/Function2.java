package com.mycompany.termproject;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Function2 is responsible for aggregating data from an S3 bucket into a single
 * file and placing that new aggregated data into a new S3 bucket.
 * 
 * @author Paul Schmidt
 */
public class Function2 implements RequestHandler<Request, HashMap<String, Object>> {
    
    /**
     * Function responsible for cleaning, stripping, and aggregating information.
     * 
     * @param request The Request object passed to this function when called.
     * Use a json object with a parameter "bucketname".
     * @param context The Context for the given function call.
     * @return Returns a response in the form of a HashMap<String, Object>
     * (Default is null at the moment)
     */
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        // Get the name of the bucket sent with the request to this function.
        String bucketname = request.getBucketname();
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        
        // Get the names of all files in the bucket.
        ObjectListing response = s3Client.listObjects(bucketname);
        List<S3ObjectSummary> objectSummaries = response.getObjectSummaries();
        List<String> fileNames = new ArrayList<String>();
        for(S3ObjectSummary objSum : objectSummaries) {
            System.out.println(objSum.getKey());
            fileNames.add(objSum.getKey());
        }
        
        // Append contents of all files to a single file.
        StringWriter sw = new StringWriter();
        for(String fileName : fileNames) {
            S3Object s3File = s3Client.getObject(new GetObjectRequest(bucketname, fileName));
            appendToFile(sw, s3File);
        }
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        
        // Set the name of the new file using java.util.Date() as the name
        String newCSVName = new java.util.Date().toString() + ".csv";
        
        // Put the new file into the new bucket.
        s3Client.putObject("bucket-stage-two", newCSVName, is, meta);
//
//        //get object file using source bucket and srcKey name
//        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        ListObjectsV2Result checkBucketSize = s3Client.listObjectsV2("bucket-stage-two");
        System.out.println(checkBucketSize.getKeyCount());
        return null;
    }
    
    public static void main(String[] args) {
        System.out.println("Hello World!");
        //generateFiles();
        Request r = new Request();
        r.setBucketname("bucket-stage-one");
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };
        Function2 tp = new Function2();
        tp.handleRequest(r, c);
        
    }
    
    /** 
     * Used for testing purposes (grabs the first 1000 lines of the data from
     * local storage, then splits into 10 different files).
     */
    private void generateFiles() {
        File csv = new File("Data/sales.csv");
        Scanner s = null;
        try {
            s = new Scanner(csv);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Function2.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (s != null) {
            s.nextLine();
            for (int i = 0; i < 10; i++) {
                FileWriter out = null;
                try {
                    out = new FileWriter(i + ".csv");
                } catch (IOException ex) {
                    Logger.getLogger(Function2.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (out != null) {
                    try {
                        for(int line = 0; line < 100; line++)
                            out.write(s.nextLine() + "\n");
                    } catch (IOException ex) {
                        Logger.getLogger(Function2.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                try {
                    if (out != null)
                        out.close();
                } catch (IOException ex) {
                    Logger.getLogger(Function2.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    /**
     * Appends the data from the S3Object after cleaning and stripping to the 
     * new StringWriter object.
     * @param sw The StringWriter object to append data to.
     * @param s3File The S3Object that contains the data to be appended.
     */
    private void appendToFile(StringWriter sw, S3Object s3File) {
        InputStream objectData = s3File.getObjectContent();
        Scanner s = new Scanner(objectData);
        while(s.hasNextLine()) {
            String line = s.nextLine();
            line = stripData(line);
            sw.append(line + "\n");
        }
    }
    
    /**
     * Strips the unwanted column data from the raw data csv forma.
     * @param line The line of csv data to strip
     * @return The new data in csv format without the unwanted column data
     */
    private String stripData(String line) {
        String[] values = line.split(",");
        String stripped = values[2];
        stripped += ", " + values[3];
        stripped += ", " + values[4];
        return stripped;
    }
    private void cleanData() {
        
    }

}

package termproject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Scanner;
import saaf.Inspector;


/**
 * Function2 is responsible for aggregating data from an S3 bucket into a single
 * file and placing that new aggregated data into a new S3 bucket.
 * 
 * @author Paul Schmidt
 */
public class Function2 implements RequestHandler<Request, HashMap<String, Object>> {
    
    private static final String DB_URL = "";
    private static final String USER = "";
    private static final String PW = "";
    /**
     * Function responsible for moving a record from an S3 CSV file into an AWS 
     * Aurora MySQL Serverless Database.
     * 
     * @param request The Request object passed to this function when called.
     * Use a JSON object with parameters "bucketname" and "filename".
     * @param context The Context for the given function call.
     * @return Returns a response from an Inspector in the form of a 
     * HashMap<String, Object> of Key/Value pairs
     */
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        // Create logger
        LambdaLogger logger = context.getLogger();    
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        // Get the name of the bucket sent with the request to this function.
        String bucketname = request.getBucketname();
        String fileName = request.getFilename();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        
        // Get Record from the S3 File
        S3Object s3File = s3Client.getObject(new GetObjectRequest(bucketname, fileName));
        InputStream objectData = s3File.getObjectContent();
        Scanner s = new Scanner(objectData);
        
        // Attempt connection to the Database
        boolean connected = false;
        Connection con = null;
        try {
            con = DriverManager.getConnection(DB_URL, USER, PW);
            connected = true;
            System.out.println("SUCCESS");
            logger.log("SUCCESS");
        } catch (SQLException ex) {
            logger.log("ERROR CONNECTING TO DATABASE");
            ex.printStackTrace();
        }
        if(connected) {
            try {
                // Bypass header
                if(s.hasNextLine()) {
                    s.nextLine();
                }
                while(s.hasNextLine()) {
                    String record = s.nextLine();
                    String insertStatement = prepareInsert(record);
                    PreparedStatement ps = con.prepareStatement(insertStatement);
                    ps.execute();
                }
                System.out.println("DISCONNECTING");
                con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
                logger.log(ex.toString());
            }
        }
        return inspector.finish();
    }
    /**
     * Prepares an INSERT statement for the database given the comma separated String.
     * 
     * @param theRecord the comma separated string that represents the record to 
     * be inserted into the database
     * @return the formatted INSERT MySWL statement as a String
     */
    private String prepareInsert(final String theRecord) {
        String[] vals = theRecord.split(",");
        StringBuilder sb = new StringBuilder();
        sb.append("insert into data values (");
        for(int i = 0; i < vals.length - 1; i++) {
            if(i == 5 || i == 7) {
                sb.append("\'");
                sb.append(transformDate(vals[i]));
                sb.append("\', ");
            } else {
                sb.append("\'");
                sb.append(vals[i]);
                sb.append("\', ");
            }

        }
        sb.append("\'");
        sb.append(vals[vals.length-1]);
        sb.append("\')");
        return sb.toString();
    }
    /**
     * Transforms the given date of the form mm/dd/yyyy to yyyy/mm/dd for use in 
     * MySQL INSERT statements.
     * 
     * @param theDate the date to transform
     * @return the reformatted date
     */
    private String transformDate(final String theDate) {
        String[] vals = theDate.split("/");
        return vals[2] + "-" + vals[0] + "-" + vals[1];
    }
}
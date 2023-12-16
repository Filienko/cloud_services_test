package test;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;


/**
 * ClearDB aids in the ability to test the state of the database after performing
 * pipeline .
 * 
 * @author Paul Schmidt
 */
public class ClearDB implements RequestHandler<Request, HashMap<String, Object>> {
    
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

        // Attempt connection to the Database
        boolean connected = false;
        Connection con = null;
        try {
            con = DriverManager.getConnection(DB_URL, USER, PW);
            connected = true;
            logger.log("SUCCESSFULLY CONNECTED TO DATABASE");
        } catch (SQLException ex) {
            logger.log("ERROR CONNECTING TO DATABASE");
            ex.printStackTrace();
        }
        if(connected) {
            try {
                // Clear table entries for testing purposes
//                System.out.println("BEFORE EMPTYING TABLE");
//                viewTable(con);
                clearTableEntries(con);
                System.out.println("AFTER EMPTYING TABLE");
                viewTable(con);
                con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
                logger.log(ex.toString());
            }
        }
        return null;
    }

    /**
     * Execute statement to clear the table for testing of the same data.
     * Alter String s where necessary for database instance.
     * @param con The connection to the Database
     */
    private void clearTableEntries(final Connection con) {
        String s = "delete from data where Units_Sold > 0";
        try {
            PreparedStatement ps = con.prepareStatement(s);
            ps.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    /**
     * Queries the database for data table and prints the results. Useful for
     * checking the current state of the data.
     * @param con  The connection to the Database.
     */
    private static void viewTable(final Connection con) {
        String s = "select * from data";
        try {
            PreparedStatement ps = con.prepareStatement(s);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                System.out.print(rsmd.getColumnName(i) + ", ");
            }
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    System.out.print(rs.getObject(i) + ", ");
                }
                System.out.println();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    public static void main(String[] args) {
        {
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

        // Create an instance of the class
        ClearDB lt = new ClearDB();

        // Create a request object
        Request req = new Request();

        // Run the function
        lt.handleRequest(req, c);

        }
    }
}
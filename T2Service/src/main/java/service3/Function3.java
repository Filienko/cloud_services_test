package service3;

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
 * Function3 is responsible for performing aggregated queries on a database.
 * 
 * @author Paul Schmidt
 */
public class Function3 implements RequestHandler<Request, HashMap<String, Object>> {
    
    private static final String DB_URL = "";
    private static final String USER = "";
    private static final String PW = "";
    /**
     * Function responsible for performing an aggregated query on a database.
     * 
     * @param request The Request object passed to this function when called.
     * @param context The Context for the given function call.
     * @return Returns a response from an Inspector in the form of a 
     * HashMap<String, Object> of Key/Value pairs
     */
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        // Create logger
        LambdaLogger logger = context.getLogger();    
        
        //Collect inital data.
        String function = request.getFunction();
        String f_col = request.getF_column();
        String w_col = request.getW_column();
        String equality = request.getEquality();
        String w_value = request.getW_value();
        String gb_col = request.getGb_column();
        logger.log("Function: " + function + " " + 
                "Function Column: " + f_col + " " +
                "Where Column: " + w_col + " " +
                "Equality: " + equality + " " +
                "WHERE Value: " + w_value + " " +
                "GroupBy Column: " + gb_col + "\n");
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
        HashMap<String, Object> results = new HashMap<String, Object>();
        if(connected) {
            try {
                String sqlFUN = "";
                switch(function.toLowerCase()) {
                    case "sum":
                        sqlFUN = "SUM(";
                        break;
                    case "avg":
                        sqlFUN = "AVG(";
                        break;
                    case "min":
                        sqlFUN = "MIN(";
                        break;
                    case "max":
                        sqlFUN = "MAX(";
                        break;
                    case "count":
                        sqlFUN = "COUNT(";
                        break;
                    default:
                        sqlFUN = "INVALID";
                }
                String eq = "";
                switch(equality) {
                    case "<":
                        eq = equality;
                        break;
                    case "<=":
                        eq = equality;
                        break;
                    case "=":
                        eq = equality;
                        break;
                    case ">=":
                        eq = equality;
                        break;
                    case ">":
                        eq = equality;
                        break;
                    default:
                        eq = "INVALID";
                        break;
                }
                String statement = "SELECT " +
                        sqlFUN + f_col + "), " + gb_col + " FROM data WHERE " + 
                        w_col + " " + eq + " " + w_value +
                        " GROUP BY " + gb_col + ";";
                logger.log("\n" + statement + "\n");
                // Clear table entries for testing purposes
                PreparedStatement ps = con.prepareStatement(statement);
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
                    results.put((String)rs.getObject(2), rs.getObject(1));
                    System.out.println();
                }
                con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
                logger.log(ex.toString());
            }
        }
        return results;
    }
}
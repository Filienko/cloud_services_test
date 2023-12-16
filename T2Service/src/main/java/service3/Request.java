package service3;

import termproject.*;

/**
 * Manages the Request sent with a Lambda Function call.
 * 
 * @author Wes Lloyd
 * @author Paul Schmidt
 */
public class Request {

    /**
     * @return the function
     */
    public String getFunction() {
        return function;
    }

    /**
     * @param function the function to set
     */
    public void setFunction(String function) {
        this.function = function;
    }

    /**
     * @return the f_column
     */
    public String getF_column() {
        return f_column;
    }

    /**
     * @param f_column the f_column to set
     */
    public void setF_column(String f_column) {
        this.f_column = f_column;
    }

    /**
     * @return the w_column
     */
    public String getW_column() {
        return w_column;
    }

    /**
     * @param w_column the w_column to set
     */
    public void setW_column(String w_column) {
        this.w_column = w_column;
    }

    /**
     * @return the equality
     */
    public String getEquality() {
        return equality;
    }

    /**
     * @param equality the equality to set
     */
    public void setEquality(String equality) {
        this.equality = equality;
    }

    /**
     * @return the w_value
     */
    public String getW_value() {
        return w_value;
    }

    /**
     * @param w_value the w_value to set
     */
    public void setW_value(String w_value) {
        this.w_value = w_value;
    }

    /**
     * @return the gb_column
     */
    public String getGb_column() {
        return gb_column;
    }

    /**
     * @param gb_column the gb_column to set
     */
    public void setGb_column(String gb_column) {
        this.gb_column = gb_column;
    }

    private String function;
    private String f_column;
    private String w_column;
    private String equality;
    private String w_value;
    private String gb_column;

    public Request() {

    }

    

}

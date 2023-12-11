package termproject;

/**
 * Manages the Request sent with a Lambda Function call.
 * 
 * @author Wes Lloyd
 * @author Paul Schmidt
 */
public class Request {

    private String bucketname;
    private String filename;

    public Request() {

    }

    /**
     * @return the bucketname
     */
    public String getBucketname() {
        return bucketname;
    }

    /**
     * @param bucketname the bucketname to set
     */
    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

}

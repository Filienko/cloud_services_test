package test;

/**
 *
 * @author Wes Lloyd
 *@author Tyler Robinson
 */
public class Request {
    private String bucketname;
    private String filename;
    private String data;


    String name;

    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

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

    


    /**
     * @return the CsvData
     */
    public String getData() {
        return data;
    }

    /**
     * @param CsvData the CsvData to set
     */
    public void setData(String CsvData) {
        this.data = CsvData;
    }
}

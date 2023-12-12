package lambda;
public class Request {
    private String bucketname;
    private String filename;
    private String data;


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
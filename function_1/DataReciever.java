package lambda;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import saaf.Inspector;
import saaf.Response;

public class DataReciever implements RequestHandler<Request, HashMap<String, Object>> {

    
    private static List<String> csvEntries = new ArrayList<>();

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        Inspector inspector = new Inspector();
       
        String csvData = request.getCsvData();
        String bucketname = request.getBucketname();

       
        csvEntries.add(csvData);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
        String timestamp = dateFormat.format(new Date());
        String filename = request.getFilename() + timestamp + ".csv";
      
           
            String combinedCsvData = String.join("\n", csvEntries);

            
            byte[] bytes = combinedCsvData.getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);

            
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain");

     
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            s3Client.putObject(new PutObjectRequest(bucketname, filename, is, meta));

            
            csvEntries.clear();
        

        Response response = new Response();
        response.setValue("Bucket:" + bucketname + " filename:" + filename);

        inspector.consumeResponse(response);

      
        return inspector.finish();
    }
}

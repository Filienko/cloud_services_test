package lambda;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.csv.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import saaf.Inspector;
import saaf.Response;


public class DataReciever implements RequestHandler<Request, HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        String csvData = request.getData();
        String bucketname = request.getBucketname();
        
        StringBuilder modifiedCsvData = new StringBuilder();
        modifiedCsvData.append("Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,"
                + "Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit,Processing Time,Gross Margin\n");

        try (Reader reader = new StringReader(csvData);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord csvRecord : csvParser) {
         
                String orderDateStr = csvRecord.get("Order Date");
                String shipDateStr = csvRecord.get("Ship Date");
                String orderPriority = csvRecord.get("Order Priority");
                String totalRevenueStr = csvRecord.get("Total Revenue");
                String totalProfitStr = csvRecord.get("Total Profit");

      
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date orderDate = dateFormat.parse(orderDateStr);
                Date shipDate = dateFormat.parse(shipDateStr);
                long processingTime = (shipDate.getTime() - orderDate.getTime()) / (1000 * 60 * 60 * 24);
                String transformedPriority = transformPriority(orderPriority);
                double totalRevenue = Double.parseDouble(totalRevenueStr);
                double totalProfit = Double.parseDouble(totalProfitStr);
                double grossMargin = totalProfit / totalRevenue;
                StringBuilder recordString = new StringBuilder();
                 for (int i = 0; i < csvRecord.size(); i++) {
                    if (csvRecord.getParser().getHeaderMap().get("Order Priority") == i) {
        
                        recordString.append(transformedPriority);
                    } else {
         
                        recordString.append(csvRecord.get(i));
                    }
                    if (i < csvRecord.size() - 1) {
                        recordString.append(",");
                    }
                }

      
                recordString.append(",")
                            .append(processingTime)
                            .append(",")
                            .append(grossMargin)
                            .append("\n");
                   modifiedCsvData.append(recordString.toString());

            }

      
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
            String timestamp = dateFormat.format(new Date());
            String filename = request.getFilename() + timestamp + ".csv";

            byte[] bytes = modifiedCsvData.toString().getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);

            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain");

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            s3Client.putObject(new PutObjectRequest(bucketname, filename, is, meta));

            Response response = new Response();
            response.setValue("Bucket:" + bucketname + " filename:" + filename);
            inspector.consumeResponse(response);

        } catch (Exception e) {
            context.getLogger().log("Error processing CSV: " + e.getMessage());
        }
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private String transformPriority(String priority) {
        switch (priority) {
            case "L": return "Low";
            case "M": return "Medium";
            case "H": return "High";
            case "C": return "Critical";
            default: return "Unknown";
        }
    }
}

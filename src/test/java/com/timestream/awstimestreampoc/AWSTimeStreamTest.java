package com.timestream.awstimestreampoc;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class AWSTimeStreamTest {

    private static AmazonTimestreamWrite buildWriteClient() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        return AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion("us-east-2")
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    private void postTimeData(){

        AmazonTimestreamWrite amazonTimestreamWrite = buildWriteClient();

        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = amazonTimestreamWrite.listDatabases(request);

        for (Database db : result.getDatabases()) {
            System.out.println(db.getDatabaseName());
        }

        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = new Dimension().withName("region").withValue("us-east-2");
        final Dimension hostname = new Dimension().withName("hostname").withValue("roger-localhost");
        final Dimension tenant = new Dimension().withName("tenant").withValue("roger2");

        dimensions.add(region);
        dimensions.add(hostname);
        dimensions.add(tenant);

        Record transactionTime = new Record()
                .withDimensions(dimensions)
                .withMeasureName("transaction_time")
                .withMeasureValue(String.valueOf(Math.random() * 100))
                .withMeasureValueType(MeasureValueType.DOUBLE)
                .withTime(String.valueOf(time));

        records.add(transactionTime);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName("roger-test")
                .withTableName("roger-table1")
                .withRecords(records);

        try {
            WriteRecordsResult writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            System.out.println("RejectedRecords: " + e);
            for (RejectedRecord rejectedRecord : e.getRejectedRecords()) {
                System.out.println("Rejected Index " + rejectedRecord.getRecordIndex() + ": "
                        + rejectedRecord.getReason());
            }
            System.out.println("Other records were written successfully. ");
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    @Test
    public void testTimestream() throws InterruptedException {

        for(int i = 0; i < 100; i++){
            postTimeData();
            Thread.sleep(1000);
        }
    }
}

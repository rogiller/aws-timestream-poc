package com.timestream.awstimestreampoc;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.Database;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesRequest;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesResult;
import org.junit.jupiter.api.Test;

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

    @Test
    public void testTimestream(){

        AmazonTimestreamWrite amazonTimestreamWrite = buildWriteClient();

        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = amazonTimestreamWrite.listDatabases(request);

        for (Database db : result.getDatabases()) {
            System.out.println(db.getDatabaseName());
        }


    }
}

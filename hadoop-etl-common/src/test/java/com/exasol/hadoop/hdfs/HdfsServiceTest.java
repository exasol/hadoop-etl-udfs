package com.exasol.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsServiceTest {

    @Test public void exceptionMessagesWhenHdfsIsNotAccessible() {

        try {
            HdfsService.getFileSystem(asList("hdfs://gibtsnicht", "broken:url"), new Configuration());

        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Contains exception message for first URL",
                    message.contains("UnknownHostException: gibtsnicht"));
            assertTrue("Contains exception message for second URL",
                    message.contains("IOException: No FileSystem for scheme: broken"));

            String causeMessage = e.getCause().getMessage();
            assertTrue("Exception on last URL will be shown as exception cause.",
                    causeMessage.contains("No FileSystem for scheme: broken"));
        }

    }

}

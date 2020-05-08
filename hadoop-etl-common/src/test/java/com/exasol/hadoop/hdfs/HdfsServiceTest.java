package com.exasol.hadoop.hdfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsServiceTest {

    @Test
    public void exceptionMessagesWhenHdfsIsNotAccessible() {
        try {
            final List<String> urls = new ArrayList<>(Arrays.asList("hdfs://gibtsnicht", "broken:url"));
            HdfsService.getFileSystem(urls, new Configuration());
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Contains exception message for first URL",
                    message.contains("UnknownHostException: gibtsnicht"));
            assertTrue("Contains exception message for second URL",
                    message.contains("UnsupportedFileSystemException: No FileSystem for scheme \"broken\""));

            String causeMessage = e.getCause().getMessage();
            assertTrue("Exception on last URL will be shown as exception cause.",
                    causeMessage.contains("No FileSystem for scheme \"broken\""));
        }

    }

}

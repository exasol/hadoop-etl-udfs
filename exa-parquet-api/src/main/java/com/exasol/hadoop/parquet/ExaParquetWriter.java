package com.exasol.hadoop.parquet;

import java.io.IOException;

public interface ExaParquetWriter {

    /**
     * Writes the current row.
     */
    void write() throws Exception;

    /**
     * Accesses next row to write.
     * Returns true, if the row was incremented.
     * Returns false, if there were no more rows.
     */
    boolean next() throws Exception;

    /**
     * Closes the Writer.
     */
    void close() throws IOException;
}

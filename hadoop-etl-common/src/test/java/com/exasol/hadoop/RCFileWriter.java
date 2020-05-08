package com.exasol.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;

/**
 * A helper class that writes single column values into Record Columnar File (RCFile) format file.
 *
 * The column values are single type values specified by {@link RCFormatFileWriter.Type}.
 */
public final class RCFileWriter {

    private final Configuration conf;
    private final FileSystem fileSystem;
    private final Type type;

    public RCFileWriter(final Configuration conf, final FileSystem fs, final Type type) {
        this.conf = conf;
        this.fileSystem = fs;
        this.type = type;
    }

    public enum Type {
        DATE {
            @Override
            public String getColumnName() {
                return "col_date";
            }

            @Override
            public String getColumnType() {
                return "date";
            }

            @Override
            public ObjectInspector getJavaObjectInspector() {
                return javaDateObjectInspector;
            }

            @Override
            public Serializer createSerializer() {
                return getSerializer(getProperties());
            }
        },
        TIMESTAMP {
            @Override
            public String getColumnName() {
                return "col_timestamp";
            }

            @Override
            public String getColumnType() {
                return "timestamp";
            }

            @Override
            public ObjectInspector getJavaObjectInspector() {
                return javaTimestampObjectInspector;
            }

            @Override
            public Serializer createSerializer() {
                return getSerializer(getProperties());
            }
        };

        protected Properties getProperties() {
            Properties properties = new Properties();
            properties.setProperty("columns", getColumnName());
            properties.setProperty("columns.types", getColumnType());
            properties.setProperty("file.inputformat", RCFileInputFormat.class.getName());
            return properties;
        }

        protected Serializer getSerializer(final Properties properties) {
            try {
                ColumnarSerDe columnarSerDe = new ColumnarSerDe();
                columnarSerDe.initialize(new JobConf(false), properties);
                return columnarSerDe;
            } catch (SerDeException exception) {
                throw new RuntimeException(exception);
            }
        }

        public abstract String getColumnName();
        public abstract String getColumnType();
        public abstract ObjectInspector getJavaObjectInspector();
        public abstract Serializer createSerializer();
    }

    public void writeValues(final File outputFile, final List<?> values) throws Exception {
        final ObjectInspector columnObjectInspector = this.type.getJavaObjectInspector();
        final SettableStructObjectInspector structObjectInspector = ObjectInspectorFactory
            .getStandardStructObjectInspector(Arrays.asList(this.type.getColumnName()),
                    Arrays.asList(columnObjectInspector));
        final Serializer serializer = this.type.createSerializer();

        final Object row = structObjectInspector.create();
        final List<StructField> fields = new ArrayList<>(
                structObjectInspector.getAllStructFieldRefs());
        final RecordWriter recordWriter = createRCFileWriter(outputFile, columnObjectInspector);

        for (final Object object : values) {
           final Object value = preprocessValue(object);
           structObjectInspector.setStructFieldData(row, fields.get(0), value);
           final Writable record = serializer.serialize(row, structObjectInspector);
           recordWriter.write(record);
       }

       recordWriter.close(false);
    }

    private RecordWriter createRCFileWriter(final File outputFile,
            final ObjectInspector columnObjectInspector) throws IOException {
        JobConf jobConf = new JobConf(false);
        return new RCFileOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                true,
                this.type.getProperties(),
                () -> {}
        );
    }

    private Object preprocessValue(final Object value) {
        if (value == null) {
            return null;
        }

        if (this.type == Type.DATE) {
            return (Date)value;
        }
        if (this.type == Type.TIMESTAMP) {
            return (Timestamp)value;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

}

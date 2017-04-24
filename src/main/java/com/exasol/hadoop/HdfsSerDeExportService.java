package com.exasol.hadoop;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.jsonpath.OutputColumnSpec;
import com.exasol.utils.UdfUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;

import com.exasol.ExaIterator;
import org.datanucleus.store.types.backed.*;

/**
 * org.apache.hadoop.mapred is old, ...mapreduce is new. However, mapred was undeprecated
 * http://stackoverflow.com/questions/7598422/is-it-better-to-use-the-mapred-or-the-mapreduce-package-to-create-a-hadoop-job
 *
 * Note that org.apache.hadoop.hive.serde is the deprecated old SerDe library. Please look at org.apache.hadoop.hive.serde2 for the latest version.
 * https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide#DeveloperGuide-HiveSerDe
 *
 * Missing features (ordered by priority):
 * TODO Re-enable single file import
 * TODO Run tests with many different datatypes, partitions, compression, etc.
 * TODO Understand security workarounds. Test Kerberos.
 * TODO Reintroduce column selection during import (deserialize only selected columns)
 * TODO Return partitions with correct data types (or give option to do so)
 * TODO Make Export generic (to use any kind of SerDe)
 *
 * Refactoring:
 * TODO Write Unit Tests
 * TODO Make ExaIterator (and all other classes) available as Interface
 *
 *
 *
 * See HCatCreateTableDesc.Builder
 */
public class HdfsSerDeExportService {

    // TODO Refactor and remove this
    private static List<Integer> cols = null;
    private static int numCols = 0;

    public static void exportToTableTest(
            final String hdfsUrl,
            final String hdfsUser,
            final String file,
            final HCatTableMetadata tableMeta) throws Exception {

        System.out.println("----------\nStarted Export Table To Hive\n----------");

        // See
        // https://ballsandbins.wordpress.com/2014/10/26/jdbc-storage-handler-for-hive-part-i/
        // https://cwiki.apache.org/confluence/display/Hive/SerDe
        // http://www.congiu.com/a-json-readwrite-serde-for-hive/
        //
        // Process
        // - Hive row/record object: This is what Hive operators work with, but to interpret it you need an ObjectInspector (which might deserialize from the internal representation)
        // - ObjectInspector: A (probably configured) ObjectInspector instance stands for
        //   a specific (Hive) type and a specific way to store the data of that type in the memory (as a Hive row object)
        //   When you want to access the real values, in a standardized format, Objectinspectors act as a deserializer
        // - StructObjectInspector: Has special method isSettable
        // - PrimitiveObjectInspector: getJavaPrimitiveClass() and getPrimitiveJavaObject()
        // - StringObjectInspector extends PrimitiveObjectInspector
        // - StructField: Returned by StructObjectInspector.getAllStructFieldRefs(). Contains field ObjectInspector and name (and comment)
        // - SerDe: interface SerDe extends Deserializer, Serializer
        //   Deserializer.deserialize(Writable):
        //     Deserializes Writable to a format which then used internally by Hive operators. Hive operators don't work with deserialized object directly, but only through ObjectInspector
        //     Call Deserializer.getObjectInspector() afterwards to get the ObjectInspector for returned object.
        //     Representation/ObjectInspector is hardcoded in Deserializer.
        //   Serializer.serialize(Object, ObjectInspector): does not matter which row (Object) representation we use as input, as long as we pass the according ObjectInspector! I.e. does not need to be the ObjectInspector returned by the deserializer
        // - Outputformat: Each OutputFormat has a RecordWriter. key is ignored during import, and so probably can be set to null. value contains the output of SerDe.serialize

        final Properties props = new Properties();
        final Configuration conf = new Configuration();
        HdfsSerDeImportService.initProperties(props, conf, tableMeta.getColumns(), Lists.<OutputColumnSpec>newArrayList());
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                final OutputFormat<?, ?> outputFormat = (OutputFormat<?, ?>) UdfUtils.getInstanceByName(tableMeta.getOutputFormatClass());
                SerDe serDe = (SerDe) UdfUtils.getInstanceByName(tableMeta.getSerDeClass());
                //ColumnarSerDe serDe = new ColumnarSerDe();
                conf.set(JobContext.TASK_OUTPUT_DIR, "dummy");
                conf.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, "dummy");
                serDe.initialize(conf, props);  // TODO .initialize is from deserializer, should not be needed!
                // TODO not yet generic
                RCFileOutputFormat.setColumnNumber(conf, tableMeta.getColumns().size());
                System.out.println("Open HDFS file system: " + hdfsUrl);
                FileSystem fs = FileSystem.get(new URI(hdfsUrl), conf);
                System.out.println("Create Writer for file: " + file);
                RecordWriter writer = outputFormat.getRecordWriter(fs, new JobConf(conf), file,null);
                System.out.println("After writer");
                ObjectInspector objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                        ImmutableList.of("artistid", "artistname", "year"),
                        ImmutableList.<ObjectInspector>of(PrimitiveObjectInspectorFactory.javaIntObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector));
                System.out.println("ObjectInspector: " + objectInspector.getClass().getName());
                StructObjectInspector structObjIns = (StructObjectInspector)objectInspector;
                for (StructField structField : structObjIns.getAllStructFieldRefs()) {
                    System.out.println("- ObjectInspector field " + structField.getFieldName() + ": " + structField.getFieldObjectInspector().getClass().getName());
                }
                // StandardStructObjectInspector allows List<Object> and Object[]
                List<Object> row = new ArrayList<>();
                row.add(1);  // artistid
                row.add("artistname"); // artistname
                row.add(2001);  // year
                writer.write(null, serDe.serialize(row, objectInspector)); // key ignored
                // Emit single row end
                writer.close(null);
                return null;
            }
        });
    }

    public static void exportTableToRCFile (
            final String hdfsUrl,
            String hdfsUser,
            final String file,
            final List<Map.Entry<String, String>> columnInfo,
            final List<Integer> selectedColumns,
            final int firstCol,
            final ExaIterator ctx) throws Exception {

        System.out.println("----------\nStarted Export Table To Hive\n----------");

        final Properties props = new Properties();
        final Configuration conf = new Configuration();
        initProperties(props, conf, columnInfo, selectedColumns);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                ColumnarSerDe rc = new ColumnarSerDe();
                rc.initialize(conf, props);
                RCFileOutputFormat.setColumnNumber(conf, numCols);
                System.out.println("Open HDFS file system: " + hdfsUrl);
                FileSystem fs = FileSystem.get(new URI(hdfsUrl), conf);
                System.out.println("Create Writer for file: " + file);
                RCFile.Writer writer = new RCFile.Writer(fs, conf, new Path(file));
                System.out.println("After writer");
                if (ctx.size() > 0) {
                    BytesRefArrayWritable brawCols = new BytesRefArrayWritable(numCols);
                    ObjectInspector objInsp = rc.getObjectInspector();
                    do {
                        for (int i = 0; i < numCols; i++) {
                            String col = ctx.getString(i + firstCol);
                            if (col != null)
                                brawCols.set(i, new BytesRefWritable(col.getBytes("UTF-8")));
                            else
                                brawCols.set(i, null);
                        }
                        ColumnarStruct row = new ColumnarStruct(objInsp, cols, new Text("NULL"));
                        row.init(brawCols);
                        writer.append((BytesRefArrayWritable) rc.serialize(row, objInsp));
                    } while (ctx.next());
                }
                writer.close();
                ctx.emit("Rows affected: " + ctx.size());
                return null;
            }
        }); // security workaround
    }

    public static void initProperties(Properties props, Configuration conf, List<Map.Entry<String, String>> columnInfo, List<Integer> selectedColumns) throws Exception {
        String colNames = "";
        String colTypes = "";
        for (Map.Entry<String, String> colInfo : columnInfo) {
            colNames += colInfo.getKey() + ",";
            colTypes += colInfo.getValue() + ",";
        }
        if (colNames.length() > 0)
            colNames = colNames.substring(0, colNames.length() - 1);
        if (colTypes.length() > 0)
            colTypes = colTypes.substring(0, colTypes.length() - 1);
        props.put(Constants.LIST_COLUMNS, colNames);
        props.put(Constants.LIST_COLUMN_TYPES, colTypes);
        props.put(Constants.SERIALIZATION_NULL_FORMAT, "NULL");

        if (selectedColumns.size() > 0) {
            ColumnProjectionUtils.appendReadColumns(conf, selectedColumns);
            cols = selectedColumns;
            numCols = cols.size();
        }
        else {
            numCols = columnInfo.size();
            cols = new ArrayList<Integer>(numCols);
            for (int i = 0; i < numCols; i++) {
                cols.add(i);
            }
        }
    }
}

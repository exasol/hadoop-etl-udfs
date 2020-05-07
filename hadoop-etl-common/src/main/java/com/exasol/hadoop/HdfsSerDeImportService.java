package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.WebHCatJsonParser;
import com.exasol.hadoop.hdfs.HdfsService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import com.exasol.jsonpath.*;
import com.exasol.utils.UdfUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * Retrieves files from (web)HDFS and decodes them using the appropriate Hadoop InputFormat and Hive SerDe.
 *
 * TODO org.apache.hadoop.hive.serde2.SerDe is deprecated and should be replaced with org.apache.hadoop.hive.serde2.AbstractSerDe
 * Problem: Some SerDes like OrcInputFormat support only deprecated SerDe class (not AbstractSerDe)
 */
public class HdfsSerDeImportService {
    public static void importFiles(
            final List<String> files,
            String inputFormatClassName,
            String serDeClassName,
            String serdeParametersJson,
            String colInfoJson,
            String partitionColumnsJson,
            final String outputColumnsSpec,
            final List<String> hdfsUrls,
            final String hdfsUserOrServicePrincipal,
            final boolean useKerberos,
            final KerberosCredentials kerberosCredentials,
            final boolean enableRPCEncryption,
            final ExaIterator ctx) throws Exception {

        System.out.println("----------\nStarted importing files\n----------");
        UdfUtils.printClassPath();

        try {
            System.out.println("read column info");
            final List<HCatTableColumn> columns = WebHCatJsonParser.parseColumnArray(colInfoJson);
            final List<HCatTableColumn> partitionColumns = WebHCatJsonParser.parseColumnArray(partitionColumnsJson);
            final List<HCatSerDeParameter> serDeParameters = WebHCatJsonParser.parseSerDeParameters(serdeParametersJson);

            // Parse Output Column Spec
            final List<OutputColumnSpec> outputColumns = outputColumnsSpec.isEmpty() ?
                    OutputColumnSpecUtil.generateDefaultOutputSpecification(columns, partitionColumns)
                    : OutputColumnSpecUtil.parseOutputSpecification(outputColumnsSpec, columns, partitionColumns);

            // We need the native hadoop libraries for snappy.
            NativeHadoopLibUtils.initHadoopNativeLib();

            System.out.println("run import");
            final InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) UdfUtils.getInstanceByName(inputFormatClassName);
            final AbstractSerDe serDe = (AbstractSerDe) UdfUtils.getInstanceByName(serDeClassName);

            UserGroupInformation ugi = useKerberos ?
                    KerberosHadoopUtils.getKerberosUGI(kerberosCredentials) : UserGroupInformation.createRemoteUser(hdfsUserOrServicePrincipal);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    final Configuration conf = HdfsService.getHdfsConfiguration(useKerberos, hdfsUserOrServicePrincipal, enableRPCEncryption);
                    FileSystem fs = HdfsService.getFileSystem(hdfsUrls,conf);
                    for (String file : files) {
                        fs = importFile(fs, file, partitionColumns, inputFormat, serDe, serDeParameters, hdfsUrls, hdfsUserOrServicePrincipal, columns, outputColumns, useKerberos, enableRPCEncryption, ctx);
                    }
                    return null;
                }
            });
        } catch (Exception ex ) {
            System.out.println(UdfUtils.traceToString(ex));
            throw ex;
        }
    }

    /**
     * @return In case of an HA failover, this method might connect to a different namenode, which is then returned
     */
    public static FileSystem importFile(
            FileSystem fs,
            final String file,
            final List<HCatTableColumn> partitionColumns,
            final InputFormat<?, ?> inputFormat,
            final AbstractSerDe serde,
            final List<HCatSerDeParameter> serDeParameters,
            final List<String> hdfsUrls,
            final String hdfsUserOrServicePrincipal,
            final List<HCatTableColumn> columns,
            final List<OutputColumnSpec> outputColumns,
            final boolean useKerberos,
            final boolean enableRPCEncryption,
            final ExaIterator ctx) throws Exception {
        System.out.println("----------\nStarted importGeneric()\n----------");
        System.out.println("- file: " + file);
        System.out.println("- partitionColumns: " + partitionColumns);
        System.out.println("- serDeParameters: " + serDeParameters);
        System.out.println("- hdfsUrl: " + org.apache.commons.lang.StringUtils.join(hdfsUrls, ","));
        System.out.println("- hdfsUserOrServicePrincipal: " + hdfsUserOrServicePrincipal);
        System.out.println("- columnInfo: " + columns);
        System.out.println("- outputColumns: " + outputColumns);
        System.out.println("- useKerberos: " + useKerberos);

        final Properties props = new Properties();
        final Configuration conf = HdfsService.getHdfsConfiguration(useKerberos, hdfsUserOrServicePrincipal, enableRPCEncryption);
        for (HCatSerDeParameter prop : serDeParameters) {
            System.out.println("Add serde prop " + prop.getName() + ": " + prop.getValue());
            props.put(prop.getName(), prop.getValue());
        }
        initProperties(props, conf, columns, outputColumns);
        serde.initialize(conf, props);
        FileStatus fileStatus = null;
        // If there is a HA namenode failover while UDFs are running, it will probably crash here when retrieving the files from namenode.
        // In case of a crash, we call {@HdfsService#getFileSystem} which will get the available namenode (or it fails because all are down)
        try {
            fileStatus = fs.getFileStatus(new Path(file));
        }
        catch (IOException ex) {
            fs = HdfsService.getFileSystem(hdfsUrls,conf);
            fileStatus = fs.getFileStatus(new Path(file));
        }
        System.out.println("importing file: " + fileStatus.getPath());
        InputSplit split = new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), (String[]) null);
        CompressionCodecFactory codecs = new org.apache.hadoop.io.compress.CompressionCodecFactory(conf);
        ArrayList<Class> codecList = new ArrayList();
        try {
            codecList.add(Class.forName("com.hadoop.compression.lzo.LzoCodec"));
        } catch (ClassNotFoundException ex) { /* LZO not supported */ }
        try {
            codecList.add(Class.forName("com.hadoop.compression.lzo.LzopCodec"));
        } catch (ClassNotFoundException ex) { /* LZOP not supported */ }
        if (!codecList.isEmpty())
            CompressionCodecFactory.setCodecClasses(conf, codecList);
        JobConf job = new JobConf(conf);
        RecordReader rr = inputFormat.getRecordReader(split, job, Reporter.NULL);
        StructObjectInspector oi = (StructObjectInspector) serde.getObjectInspector();
        List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
        Object[] objs = new Object[outputColumns.size()];
        String decodedFilePath = URLDecoder.decode(file, "UTF-8"); // Convert from URL encoding. TODO Probably we should only do this for webHDFS
        setPartitionValues(decodedFilePath, columns.size(), partitionColumns, outputColumns, objs);
        Object key = rr.createKey();
        Writable value = (Writable) rr.createValue();
        JsonBuilderFactory jsonFactory = Json.createBuilderFactory(null);
        Map<Integer, List<OutputColumnSpec>> outColsForColumns = OutputColumnSpecUtil.getOutputSpecsForColumns(outputColumns);
        while (rr.next(key, value)) {
            // In complex type hierarchies with null values, some columns might not be set, so we have to set manually.
            // TODO Find a better which is optimized for the case where we have to nullify only the fields which were not already overwritten.
            for (OutputColumnSpec outCol : outputColumns) {
                if (outCol.getColumnPosition() < columns.size()) {
                    objs[outCol.getResultPosition()] = null;
                }
            }
            Object row = serde.deserialize(value);
            for (int i=0; i<columns.size(); i++) {
                if (!outColsForColumns.containsKey(i)) {
                    continue;   // this column is not emitted => skip
                }
                try {
                    Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
                    List<JsonPathElement> curPath = new ArrayList<>();
                    visitTree(fieldData, fieldRefs.get(i).getFieldObjectInspector(), curPath, outColsForColumns.get(i), objs, jsonFactory);
                } catch (ArrayIndexOutOfBoundsException e) {
                    // This is a fix for Hive Versions earlier 2.0 which don't have this path: https://issues.apache.org/jira/browse/HIVE-12475
                    // This can be reproduced by creating a Parquet table, inserting data, and afterwards adding columns.
                    // Then, when reading the files created before the columns were added, you get the Exception.
                    objs[i] = null;
                } catch (Exception e) {
                    throw new RuntimeException("Unhandled exception while getting column " + i, e);
                }
            }
            ctx.emit(objs);
        }
        rr.close();
        return fs;
    }

    private static void visitTree(
            Object data,
            ObjectInspector objInspector,
            List<JsonPathElement> curPath,
            List<OutputColumnSpec> outputColumns,
            Object[] outRow,
            JsonBuilderFactory jsonFactory) {
        if (data == null) {
            // If we have vectorized processing, we should go further visiting the tree via ObjectInspector (if any of the elements in the array has a non-null value)
            return;
        }

        // Shall we output exactly this path?
        for (OutputColumnSpec spec : outputColumns) {
            if (spec.getPath().equals(curPath)) {
                switch (objInspector.getCategory()) {
                case PRIMITIVE:
                    // We reached a leaf. Path should match completely (We could show an error if there are deeper paths for this subtree)
                    outRow[spec.getResultPosition()] = getJavaObjectFromPrimitiveData(data, objInspector);
                    break;
                case LIST:
                    outRow[spec.getResultPosition()] = getJsonArrayFromFieldData(data, objInspector, jsonFactory).build().toString();
                    break;
                case MAP:
                case STRUCT:
                case UNION:
                    outRow[spec.getResultPosition()] = getJsonObjectFromFieldData(data, objInspector, jsonFactory).build().toString();
                    break;
                }
            }
        }

        // Stop recursion if we reached a leaf
        if (objInspector.getCategory() == Category.PRIMITIVE) {
            return;
        }

        // Special case UNION: We might have multiple nested unions and should resolve them.
        while (objInspector.getCategory() == Category.UNION) {
            // Type may vary for each row, we have to be careful. If we vectorize and we find a union we have to set vector size to 1.
            UnionObjectInspector oi = (UnionObjectInspector)objInspector;
            byte tag = oi.getTag(data);
            objInspector = oi.getObjectInspectors().get(tag);
        }

        // Check if we need to go deeper, i.e. if there is any output column with a path below the current path
        // We could make this more intelligent, to check where we need to go down below.
        boolean needGoDown = false;
        for (OutputColumnSpec spec : outputColumns) {
            if (OutputColumnSpecUtil.leftIsPrefixOfRightPath(curPath, spec.getPath())) {
                needGoDown = true;
            }
        }
        if (!needGoDown) {
            return;
        }

        // Go deeper in the data type hierarchy if required
        switch (objInspector.getCategory()) {
        case LIST: {
            ListObjectInspector oi = (ListObjectInspector)objInspector;
            List<?> list = oi.getList(data);
            ObjectInspector elementInsp = oi.getListElementObjectInspector();
            for (int i=0; i<list.size(); i++) {
                Object elementData = list.get(i);
                if (elementData == null) {
                    continue;
                }
                curPath.add(new JsonPathListIndexElement(i));
                visitTree(elementData, elementInsp, curPath, outputColumns, outRow, jsonFactory);
                curPath.remove(curPath.size()-1);
            }
            break;
        }
        case MAP: {
            MapObjectInspector oi = (MapObjectInspector)objInspector;
            Map<?, ?> mapData = oi.getMap(data);
            ObjectInspector keyInspector = oi.getMapKeyObjectInspector();
            for (Map.Entry<?, ?> mapEntryData : mapData.entrySet()) {
                if (keyInspector.getCategory() != Category.PRIMITIVE) {
                    throw new RuntimeException("Hive map key is not a primitive type: " + keyInspector.getCategory());
                }
                String key = getJavaObjectFromFieldData(mapEntryData.getKey(), keyInspector).toString();
                curPath.add(new JsonPathFieldElement(key));
                visitTree(mapEntryData.getValue(), oi.getMapValueObjectInspector(), curPath, outputColumns, outRow, jsonFactory);
                curPath.remove(curPath.size()-1);
            }
            break;
        }
        case STRUCT: {
            StructObjectInspector oi = (StructObjectInspector)objInspector;
            List<? extends StructField> structFields = oi.getAllStructFieldRefs();
            List<?> structFieldsData = oi.getStructFieldsDataAsList(data);
            for (int i = 0; i < structFields.size(); i++) {
                StructField field = structFields.get(i);
                String name = field.getFieldName();
                curPath.add(new JsonPathFieldElement(name));
                visitTree(structFieldsData.get(i), field.getFieldObjectInspector(), curPath, outputColumns, outRow, jsonFactory);
                curPath.remove(curPath.size()-1);
            }
            break;
        }
        case PRIMITIVE:
        case UNION:
        default: {
            throw new RuntimeException("Unexpected category " + objInspector.getCategory() + ", should never happen");
        }
        }
    }

    private static Object getJavaObjectFromPrimitiveData(Object data, ObjectInspector objInsp) {
        assert(objInsp.getCategory() == Category.PRIMITIVE);
        if (data == null) {
            return null;
        }
        if (data instanceof BytesWritable && objInsp instanceof WritableHiveDecimalObjectInspector) {
            // BytesWritable cannot be directly cast to HiveDecimalWritable
            WritableHiveDecimalObjectInspector oi = (WritableHiveDecimalObjectInspector) objInsp;
            data = oi.create(((BytesWritable) data).getBytes(), oi.scale());
        }
        Object obj = ObjectInspectorUtils.copyToStandardJavaObject(data, objInsp);
        if (obj instanceof HiveDecimal) {
            obj = ((HiveDecimal) obj).bigDecimalValue();
        } else if (obj instanceof HiveVarchar || obj instanceof HiveChar) {
            obj = obj.toString();
        } else if (obj instanceof byte[]) {
            obj = Hex.encodeHexString((byte[]) obj);
        }
        if (obj instanceof Date) {
            final Date hiveDate = (Date)obj;
            return new java.sql.Date(hiveDate.toEpochMilli());
        }
        if (obj instanceof Timestamp) {
            final Timestamp hiveTimestamp = (Timestamp)obj;
            return hiveTimestamp.toSqlTimestamp();
        }
        return obj;
    }

    private static void initProperties(
            Properties props,
            Configuration conf,
            List<HCatTableColumn> columns,
            List<OutputColumnSpec> outputColumns) throws Exception {
        String colNames = "";
        String colTypes = "";
        for (HCatTableColumn colInfo : columns) {
            colNames += colInfo.getName() + ",";
            colTypes += colInfo.getDataType() + ",";
        }
        if (colNames.length() > 0)
            colNames = colNames.substring(0, colNames.length() - 1);
        if (colTypes.length() > 0)
            colTypes = colTypes.substring(0, colTypes.length() - 1);
        props.put(serdeConstants.LIST_COLUMNS, colNames);
        props.put(serdeConstants.LIST_COLUMN_TYPES, colTypes);
        props.put(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
        // Fix for Avro (NullPointerException if null)
        if (props.getProperty("columns.comments") == null) {
            props.put("columns.comments", "");
        }
        // Pushdown projection if we don't need all columns
        Set<Integer> requiredColumns = new HashSet<>();
        for (OutputColumnSpec spec : outputColumns) {
            if (spec.getColumnPosition() < columns.size()) {
                requiredColumns.add(spec.getColumnPosition());
            }
        }
        if (requiredColumns.size() < columns.size()) {
            ColumnProjectionUtils.appendReadColumns(conf, new ArrayList<>(requiredColumns));
        }
    }

    private static Object getJavaObjectFromFieldData(Object data, ObjectInspector objInsp) {
        if (data == null) {
            return null;
        }
        if (objInsp.getCategory() == Category.PRIMITIVE) {
            Object obj = ObjectInspectorUtils.copyToStandardJavaObject(data, objInsp);
            if (obj instanceof HiveDecimal) {
                obj = ((HiveDecimal) obj).bigDecimalValue();
            } else if (obj instanceof HiveVarchar || obj instanceof HiveChar) {
                obj = obj.toString();
            } else if (obj instanceof byte[]) {
                obj = Hex.encodeHexString((byte[]) obj);
            }
            return obj;
        } else if (objInsp.getCategory() == Category.LIST) {
            return getJsonArrayFromFieldData(data, objInsp, Json.createBuilderFactory(null)).build().toString();
        } else {
            return getJsonObjectFromFieldData(data, objInsp, Json.createBuilderFactory(null)).build().toString();
        }
    }

    private static JsonArrayBuilder getJsonArrayFromFieldData(Object data, ObjectInspector objInsp, JsonBuilderFactory jsonFactory) {
        JsonArrayBuilder jab = jsonFactory.createArrayBuilder();
        ListObjectInspector oi = (ListObjectInspector) objInsp;
        List<?> list = oi.getList(data);
        ObjectInspector elemInsp = oi.getListElementObjectInspector();
        for (Object obj : list) {
            if (obj == null)
                jab.addNull();
            else if (elemInsp.getCategory() == Category.PRIMITIVE) {
                Object o = getJavaObjectFromPrimitiveData(obj, elemInsp);
                if (o instanceof Integer || o instanceof Short || o instanceof Byte)
                    jab.add((Integer) o);
                else if (o instanceof Long)
                    jab.add((Long) o);
                else if (o instanceof Float || o instanceof Double)
                    jab.add((Double) o);
                else if (o instanceof BigDecimal)
                    jab.add((BigDecimal) o);
                else if (o instanceof Boolean)
                    jab.add((Boolean) o);
                else
                    jab.add(o.toString());
            }
            else if (elemInsp.getCategory() == Category.LIST) {
                jab.add(getJsonArrayFromFieldData(obj, elemInsp, jsonFactory));
            }
            else {
                jab.add(getJsonObjectFromFieldData(obj, elemInsp, jsonFactory));
            }
        }
        return jab;
    }

    private static JsonObjectBuilder getJsonObjectFromFieldData(Object data, ObjectInspector objInsp, JsonBuilderFactory jsonFactory) {
        JsonObjectBuilder job = jsonFactory.createObjectBuilder();
        switch (objInsp.getCategory()) {
        case MAP:
        {
            MapObjectInspector oi = (MapObjectInspector) objInsp;
            Map<?, ?> map = oi.getMap(data);
            ObjectInspector keyInsp = oi.getMapKeyObjectInspector();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (keyInsp.getCategory() != Category.PRIMITIVE)
                    throw new RuntimeException("Hive map key is not a primitve type: " + keyInsp.getCategory());
                String key = getJavaObjectFromFieldData(entry.getKey(), keyInsp).toString();
                addJsonObjectPair(job, key, entry.getValue(), oi.getMapValueObjectInspector(), jsonFactory);
            }
            break;
        }
        case STRUCT:
        {
            StructObjectInspector oi = (StructObjectInspector) objInsp;
            List<? extends StructField> structFields = oi.getAllStructFieldRefs();
            List<?> structValues = oi.getStructFieldsDataAsList(data);
            int size = structFields.size();
            for (int i = 0; i < size; i++) {
                StructField field = structFields.get(i);
                String name = field.getFieldName();
                addJsonObjectPair(job, name, structValues.get(i), field.getFieldObjectInspector(), jsonFactory);
            }
            break;
        }
        case UNION:
        {
            UnionObjectInspector oi = (UnionObjectInspector) objInsp;
            byte tag = oi.getTag(data);
            addJsonObjectPair(job, Byte.toString(tag), oi.getField(data), oi.getObjectInspectors().get(tag), jsonFactory);
            break;
        }
        default:
            throw new RuntimeException("Unexpected Hive type: " + objInsp.getCategory());
        }
        return job;
    }

    private static void addJsonObjectPair(JsonObjectBuilder job, String key, Object obj, ObjectInspector objInsp, JsonBuilderFactory jsonFactory) {
        if (obj == null)
            job.addNull(key);
        else if (objInsp.getCategory() == Category.PRIMITIVE) {
            Object o = getJavaObjectFromFieldData(obj, objInsp);
            if (o instanceof Integer)
                job.add(key, (Integer) o);
            else if (o instanceof Byte)
                job.add(key, (Byte) o);
            else if (o instanceof Short)
                job.add(key, (Short) o);
            else if (o instanceof Long)
                job.add(key, (Long) o);
            else if (o instanceof Float)
                job.add(key, (Float) o);
            else if (o instanceof Double)
                job.add(key, (Double) o);
            else if (o instanceof BigDecimal)
                job.add(key, (BigDecimal) o);
            else if (o instanceof Boolean)
                job.add(key, (Boolean) o);
            else
                job.add(key, o.toString());
        }
        else if (objInsp.getCategory() == Category.LIST) {
            job.add(key, getJsonArrayFromFieldData(obj, objInsp, jsonFactory));
        }
        else {
            job.add(key, getJsonObjectFromFieldData(obj, objInsp, jsonFactory));
        }
    }

    private static void setPartitionValues(String file, int numRegularColumns, List<HCatTableColumn> partitionColumns, List<OutputColumnSpec> outputColumns, Object[] objs) throws Exception {
        for (OutputColumnSpec outputColumn : outputColumns) {
            if (outputColumn.getColumnPosition() >= numRegularColumns) {
                HCatTableColumn partitionColumn = partitionColumns.get(outputColumn.getColumnPosition()-numRegularColumns);
                String partName = partitionColumn.getName();
                String partPrefix = "/" + partName + "=";
                int partIdx = file.indexOf(partPrefix);
                if (partIdx < 0) {
                    throw new RuntimeException("Hive partition '" + partName + "' could not be found in path: " + file);
                }
                partIdx += partPrefix.length();
                int slashIdx = file.indexOf('/', partIdx);
                if (slashIdx < 0) {
                    throw new RuntimeException("End of value for Hive partition '" + partName + "' could not be found in path: " + file);
                }
                String value = file.substring(partIdx, slashIdx);
                objs[outputColumn.getResultPosition()] = getJavaObjectFromHiveString(value, partitionColumn.getDataType());
            }
        }
    }

    private static Object getJavaObjectFromHiveString(String value, String type) throws Exception {
        Object obj;
        if (type.indexOf("(") > 0)
            type = type.substring(0, type.indexOf("("));
        if (type.indexOf("<") > 0)
            type = type.substring(0, type.indexOf("<"));
        String javaType = WebHdfsAndHCatService.getJavaType(type);
        if (javaType == null)
            throw new RuntimeException("Hive type '" + type + "' is unsupported for partitions");
        switch (javaType) {
        case "java.lang.String":
            obj = value;
            break;
        case "java.lang.Byte":
            obj = Byte.valueOf(value);
            break;
        case "java.lang.Short":
            obj = Short.valueOf(value);
            break;
        case "java.lang.Integer":
            obj = Integer.valueOf(value);
            break;
        case "java.lang.Long":
            obj = Long.valueOf(value);
            break;
        case "java.lang.Float":
            obj = Float.valueOf(value);
            break;
        case "java.lang.Double":
            obj = Double.valueOf(value);
            break;
        case "java.math.BigDecimal":
            obj = new BigDecimal(value);
            break;
        case "java.sql.Timestamp":
            obj = java.sql.Timestamp.valueOf(value);
            break;
        case "java.sql.Date":
            obj = java.sql.Date.valueOf(value);
            break;
        case "java.lang.Boolean":
            obj = Boolean.valueOf(value);
            break;
        default:
            throw new RuntimeException("Hive type '" + type + "' is unsupported for partitions");
        }
        return obj;
    }
}

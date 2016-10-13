package com.exasol.hadoop;

import com.exasol.utils.UdfUtils;

import java.io.*;
import java.lang.reflect.Field;

public class NativeHadoopLibUtils {
    
    public static void initHadoopNativeLib() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final String nativeHadoopLibDir = "/tmp/hadoop_etl_libs/";
        NativeHadoopLibUtils.createDirIfNotExists(nativeHadoopLibDir);
        NativeHadoopLibUtils.writeNativeHadoopLibs(nativeHadoopLibDir);
        NativeHadoopLibUtils.addDirToJavaLibraryPath(nativeHadoopLibDir);
    }

    private static void createDirIfNotExists(String dir) {
        File dirFile = new File(dir);
        if (!dirFile.exists()) {
            try {
                dirFile.mkdir();
            }
            catch(SecurityException se){
                throw se;
            }
        }
    }

    private static void writeNativeHadoopLibs(String targetLibDir) {
        try {
            writeNativeHadoopLib("libhadoop.so", targetLibDir);
            writeNativeHadoopLib("libsnappy.so", targetLibDir);
        } catch (Exception e) {
            System.out.println("Error writing the native Hadoop libraries from resources to tmp: " + e.toString());
            e.printStackTrace();
        }
    }
    
    private static void writeNativeHadoopLib(String resourceName, String targetPath) throws Exception {
        File dirFile = new File(targetPath + resourceName);
        if (dirFile.exists()) {
            // TODO Improve this so that Hadoop native lib can be updated
            return;
        }
        InputStream inStream = null;
        OutputStream outStream = null;
        try {
            inStream = UdfUtils.class.getClassLoader().getResourceAsStream(resourceName);
            if(inStream == null) {
                throw new Exception("Cannot get resource \"" + resourceName + "\" from Jar file.");
            }

            int readBytes;
            byte[] buffer = new byte[4096];
            outStream = new FileOutputStream(targetPath + resourceName, false);
            while ((readBytes = inStream.read(buffer)) > 0) {
                outStream.write(buffer, 0, readBytes);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            inStream.close();
            outStream.close();
        }
    }

    /**
     * This enables the java.library.path to be modified at runtime, which is required to load java native libraries
     * From a Sun engineer at http://forums.sun.com/thread.jspa?threadID=707176
     * See http://stackoverflow.com/questions/5419039/is-djava-library-path-equivalent-to-system-setpropertyjava-library-path
     * and http://fahdshariff.blogspot.jp/2011/08/changing-java-library-path-at-runtime.html
     */
    private static void addDirToJavaLibraryPath(String path) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        System.setProperty("java.library.path", path );
        Field fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
        fieldSysPath.setAccessible( true );
        fieldSysPath.set( null, null );
    }
    
    /**
     * This is another variant of addDirToJavaLibraryPath(). Currently unused, because the other should work.
     */
    @SuppressWarnings("unused")
    private static void addDirToJavaLibraryPath2(String s) throws IOException {
        try {
            Field field = ClassLoader.class.getDeclaredField("usr_paths");
            field.setAccessible(true);
            String[] paths = (String[])field.get(null);
            for (String path : paths) {
                if (s.equals(path)) {
                    return;
                }
            }
            String[] tmp = new String[paths.length+1];
            System.arraycopy(paths,0,tmp,0,paths.length);
            tmp[paths.length] = s;
            field.set(null,tmp);
            System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + s);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed to get permissions to set library path");
        } catch (NoSuchFieldException e) {
            throw new IOException("Failed to get field handle to set library path");
        }
    }
}

package com.exasol.hadoop;

import com.exasol.utils.UdfUtils;

import java.io.*;
import java.lang.reflect.Field;
import java.util.Map;

public class NativeHadoopLibUtils {
    
    public static void initHadoopNativeLib() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        // This path must exist at program startup and must be in LD_LIBRARY_PATH. Otherwise the dynamic linker seems to ignore it
        final String nativeHadoopLibDir = "/tmp/";
        NativeHadoopLibUtils.writeNativeHadoopLibs(nativeHadoopLibDir);
        NativeHadoopLibUtils.addDirToJavaLibraryPath(nativeHadoopLibDir);
        printEnv();
    }

    private static void printEnv() {
        Map<String, String> env = System.getenv();
        System.out.println("ENV:");
        for (String envName : env.keySet()) {
            System.out.format("- %s=%s%n",
                    envName,
                    env.get(envName));
        }
    }

    private static void writeNativeHadoopLibs(String targetLibDir) {
        try {
            writeNativeHadoopLib("libhadoop.so", "libhadoop.so", targetLibDir);
            // libhadoop.so does a dlopen("libsnappy.so.1"), so we have to use the same filename here
            writeNativeHadoopLib("libsnappy.so", "libsnappy.so.1", targetLibDir);
        } catch (Exception e) {
            System.out.println("Error writing the native Hadoop libraries from resources to tmp: " + e.toString());
            e.printStackTrace();
        }
    }
    
    private static void writeNativeHadoopLib(String resourceName, String targetFileName, String targetPath) throws Exception {
        File dirFile = new File(targetPath + targetFileName);
        if (dirFile.exists()) {
            throw new RuntimeException("Error writing resource " + targetPath + targetFileName + " for native Hadoop libraries (file already exists)");
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
            outStream = new FileOutputStream(targetPath + targetFileName, false);
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



import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.exasol.ExaConnectionInformation;

/**
 * This is only used in EXASOL 5.0 backport.
 * The class allows to access connection information from inside a UDF, which is a 6.0 feature.
 * The user can import a script, which implement a class with the getConnection() method, returning the connection information.
 */
public class ExaBackportHelper {
    
    private static final String methodName = "getConnection";
    private static final String methodSignature = "public com.exasol.ExaConnectionInformation getConnection();";

    public static ExaConnectionInformation getConnection(String className) {
        ExaConnectionInformation conn = null;
        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getMethod(methodName);
            Object object = (Object) clazz.newInstance();
            conn = (ExaConnectionInformation)method.invoke(object);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("No connection class found with name " + className + ". Consider that the class name is case-sensitive and has to include the package.", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("The class has no method " + methodName + ". Expected signature: " + methodSignature, e);
        } catch (SecurityException e) {
            throw new RuntimeException("Unexpected SecurityException while accessing method " + methodName + ". Expected signature: " + methodSignature, e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Unexpected InstantiationException while instantiating " + className, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unexpected IllegalAccessException while instantiating " + className, e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unexpected IllegalArgumentException while calling " + methodName + ". Expected signature: " + methodSignature, e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Unexpected InvocationTargetException while calling " + methodName + ". Expected signature: " + methodSignature, e);
        }
        return conn;
    }
    
}

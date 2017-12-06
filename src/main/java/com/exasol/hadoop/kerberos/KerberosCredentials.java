package com.exasol.hadoop.kerberos;

import com.exasol.ExaConnectionInformation;
import com.exasol.utils.UdfUtils;

public class KerberosCredentials {

    private String principal;   // Kerberos Principal, e.g. kerbuser@EXAMPLE.COM
    private byte[] configFile;  // Content of the kerberos config file
    private byte[] keytabFile;  // Content of the kerberos keytab file
    
    public KerberosCredentials(String principal, byte[] configFile, byte[] keytabFile) {
        this.principal = principal;
        this.configFile = configFile;
        this.keytabFile = keytabFile;
    }

    public KerberosCredentials(ExaConnectionInformation kerberosConn) {
        principal = kerberosConn.getUser();
        final String krbKey = "ExaAuthType=Kerberos";
        String[] confKeytab = kerberosConn.getPassword().split(";");
        if (confKeytab.length != 3 || !confKeytab[0].equals(krbKey)) {
            throw new RuntimeException("An invalid Kerberos CONNECTION was specified.");
        }
        configFile = UdfUtils.base64ToByteArray(confKeytab[1]);
        keytabFile = UdfUtils.base64ToByteArray(confKeytab[2]);
    }
    
    public String getPrincipal() {
        return principal;
    }
    
    public byte[] getConfigFile() {
        return configFile;
    }
    
    public byte[] getKeytabFile() {
        return keytabFile;
    }
    
}

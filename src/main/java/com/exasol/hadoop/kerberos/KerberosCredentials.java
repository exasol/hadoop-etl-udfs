package com.exasol.hadoop.kerberos;

import com.exasol.ExaConnectionInformation;
import com.exasol.utils.UdfUtils;

public class KerberosCredentials {

    private String principle;   // Kerberos Principle, e.g. kerbuser@EXAMPLE.COM
    private byte[] configFile;  // Content of the kerberos config file
    private byte[] keytabFile;  // Content of the kerberos keytab file
    
    public KerberosCredentials(String principle, byte[] configFile, byte[] keytabFile) {
        this.principle = principle;
        this.configFile = configFile;
        this.keytabFile = keytabFile;
    }

    public KerberosCredentials(ExaConnectionInformation kerberosConn) {
        principle = kerberosConn.getUser();
        final String krbKey = "ExaAuthType=Kerberos";
        String[] confKeytab = kerberosConn.getPassword().split(";");
        if (confKeytab.length != 3 || !confKeytab[0].equals(krbKey)) {
            throw new RuntimeException("An invalid Kerberos CONNECTION was specified.");
        }
        configFile = UdfUtils.base64ToByteArray(confKeytab[1]);
        keytabFile = UdfUtils.base64ToByteArray(confKeytab[2]);
    }
    
    public String getPrinciple() {
        return principle;
    }
    
    public byte[] getConfigFile() {
        return configFile;
    }
    
    public byte[] getKeytabFile() {
        return keytabFile;
    }
    
}

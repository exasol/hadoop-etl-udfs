package com.exasol.hadoop.kerberos;

public class KerberosCredentials {

    private String principle;   // Kerberos Principle, e.g. kerbuser@EXAMPLE.COM
    private byte[] configFile;  // Content of the kerberos config file
    private byte[] keytabFile;  // Content of the kerberos keytab file
    
    public KerberosCredentials(String principle, byte[] configFile, byte[] keytabFile) {
        this.principle = principle;
        this.configFile = configFile;
        this.keytabFile = keytabFile;
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

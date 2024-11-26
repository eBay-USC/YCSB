package site.ycsb.db.janusgraph;

import org.nugraph.client.config.AbstractNuGraphConfig;

public class CustomNuGraphConfig extends AbstractNuGraphConfig {
    private final String serviceIp;
    private final String failoverServiceIp;
    private final boolean ssl;
    private final String tlsAuthorityOverride;

    public CustomNuGraphConfig(String serviceHost, String failoverIp,
                               boolean ssl, String tlsAuthorityOverride) {
        this.serviceIp = serviceHost;
        this.failoverServiceIp = failoverIp;
        this.ssl = ssl;
        this.tlsAuthorityOverride = tlsAuthorityOverride;
    }

    @Override
    public String getFailoverServiceHost() {
        return failoverServiceIp;
    }

    @Override
    public String getRegion() {
        return "NA";
    }

    @Override
    public int getRestConnectTimeout() {
        return 1000;
    }

    @Override
    public int getRestReadTimeout() {
        return 1000;
    }

    @Override
    public String getServiceHost() {
        return serviceIp;
    }

    @Override
    public int getServicePort() {
        return ssl ? 8443: 8080;
    }

    @Override
    public long getTimeout() {
        return 20000;
    }

    @Override
    public boolean isPublishMetricsToSherlock() {
        return false;
    }

    @Override
    public boolean isResetTimeoutOnRetryEnabled() {
        return false;
    }

    @Override
    public boolean isSSLEnabled() {
        return ssl;
    }

    @Override
    public String getProperty(PropertyName prop) {
        if (prop.equals(PropertyName.UMP_EVENTS_ENABLED)) {
            return "false";
        } else if (prop.equals(PropertyName.UMP_NAMESPACE)) {
            return "nugraph";
        } else if (prop.equals(PropertyName.UMP_CONSUMERID)) {
//            return Utils.getConsumerId();
            return "urn:ebay-marketplace-consumerid:33110598-3c00-4901-9887-2de13a5f1e9c";
        } else if (prop.equals(PropertyName.AUTHORITY_OVERRIDE)) {
            return tlsAuthorityOverride;
        }

        return super.getProperty(prop);
    }
}

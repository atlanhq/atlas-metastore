package org.apache.atlas.featureflag;
import com.launchdarkly.sdk.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Objects;

@Component
final public class AtlasFeatureFlagClient {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasFeatureFlagClient.class);
    private final static String LAUNCH_DARKLY_SDK_KEY       = Objects.toString(System.getenv("USER_LAUNCH_DARKLY_SDK_KEY"), "");
    public final static String INSTANCE_DOMAIN_NAME         = "https://" + Objects.toString(System.getenv("DOMAIN_NAME"), "");
    public final static String UNQ_CONTEXT_KEY              = "context-atlas";
    public final static String CONTEXT_NAME                 = "Atlas";

    private static LDClient launchDarklyClient;

    public AtlasFeatureFlagClient() {
        try {
            this.launchDarklyClient = new LDClient(LAUNCH_DARKLY_SDK_KEY);
        } catch (Exception e) {
            LOG.error("Error while initializing LaunchDarkly client", e);
            throw e;
        }
    }

    public LDClient getClient() {
        return launchDarklyClient;
    }

    @PreDestroy
    public void destroy() throws IOException {
        this.launchDarklyClient.close();
    }
}
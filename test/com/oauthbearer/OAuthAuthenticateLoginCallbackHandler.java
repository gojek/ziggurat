package oauthbearer;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OAuthAuthenticateLoginCallbackHandler implements AuthenticateCallbackHandler {
    private Map<String, String> moduleOptions;

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        moduleOptions = (Map<String, String>) jaasConfigEntries.get(0).getOptions();
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleCallback((OAuthBearerTokenCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleCallback(OAuthBearerTokenCallback callback) {
        String username = moduleOptions.get("username");
        // Create a simple token - in production this would involve actual OAuth flow
        OAuthBearerToken token = new OAuthBearerToken() {
            @Override
            public String value() {
                return "dummy-token";
            }

            @Override
            public Long startTimeMs() {
                return System.currentTimeMillis();
            }

            @Override
            public long lifetimeMs() {
                return 3600000L;
            }

            @Override
            public String principalName() {
                return username;
            }

            @Override
            public Set<String> scope() {
                return Collections.emptySet();
            }
        };
        callback.token(token);
    }

    @Override
    public void close() {}
} 
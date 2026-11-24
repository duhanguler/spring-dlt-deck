package io.github.duhanguler.dltdeck.starter.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized configuration properties for {@code spring-dlt-deck}.
 */
@ConfigurationProperties(prefix = "dlt.deck")
public class DltDeckProperties {

    private boolean enabled = true;
    private String basePath = "/dlt-deck";
    private String podId = "dlt-deck-" + ProcessHandle.current().pid();
    private final Ingestion ingestion = new Ingestion();
    private final Replay replay = new Replay();
    private final Storage storage = new Storage();
    private final Security security = new Security();
    private final Ui ui = new Ui();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath.endsWith("/") && basePath.length() > 1
                ? basePath.substring(0, basePath.length() - 1)
                : basePath;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public Ingestion getIngestion() {
        return ingestion;
    }

    public Replay getReplay() {
        return replay;
    }

    public Storage getStorage() {
        return storage;
    }

    public Security getSecurity() {
        return security;
    }

    public Ui getUi() {
        return ui;
    }

    public static class Ingestion {
        private boolean enabled = true;
        private String groupId = "spring-dlt-deck-ingestor";
        private String groupProtocol = "consumer";
        private List<String> topicPatterns = new ArrayList<>(List.of(".*\\.dlt$", ".*-dlt$"));
        private List<String> allowTopics = new ArrayList<>();
        private List<String> denyTopics = new ArrayList<>(List.of("__consumer_offsets", "__transaction_state"));
        private Duration pollTimeout = Duration.ofMillis(500);
        private Duration discoveryInterval = Duration.ofSeconds(15);
        private int batchWindowSize = 100;
        private int maxPollRecords = 250;
        private int maxReplayHops = 3;
        private Duration dbBackpressureThreshold = Duration.ofSeconds(2);
        private Duration backpressurePauseDuration = Duration.ofSeconds(5);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getGroupProtocol() {
            return groupProtocol;
        }

        public void setGroupProtocol(String groupProtocol) {
            this.groupProtocol = groupProtocol;
        }

        public List<String> getTopicPatterns() {
            return topicPatterns;
        }

        public void setTopicPatterns(List<String> topicPatterns) {
            this.topicPatterns = topicPatterns;
        }

        public List<String> getAllowTopics() {
            return allowTopics;
        }

        public void setAllowTopics(List<String> allowTopics) {
            this.allowTopics = allowTopics;
        }

        public List<String> getDenyTopics() {
            return denyTopics;
        }

        public void setDenyTopics(List<String> denyTopics) {
            this.denyTopics = denyTopics;
        }

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public Duration getDiscoveryInterval() {
            return discoveryInterval;
        }

        public void setDiscoveryInterval(Duration discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
        }

        public int getBatchWindowSize() {
            return batchWindowSize;
        }

        public void setBatchWindowSize(int batchWindowSize) {
            this.batchWindowSize = batchWindowSize;
        }

        public int getMaxPollRecords() {
            return maxPollRecords;
        }

        public void setMaxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
        }

        public int getMaxReplayHops() {
            return maxReplayHops;
        }

        public void setMaxReplayHops(int maxReplayHops) {
            this.maxReplayHops = maxReplayHops;
        }

        public Duration getDbBackpressureThreshold() {
            return dbBackpressureThreshold;
        }

        public void setDbBackpressureThreshold(Duration dbBackpressureThreshold) {
            this.dbBackpressureThreshold = dbBackpressureThreshold;
        }

        public Duration getBackpressurePauseDuration() {
            return backpressurePauseDuration;
        }

        public void setBackpressurePauseDuration(Duration backpressurePauseDuration) {
            this.backpressurePauseDuration = backpressurePauseDuration;
        }
    }

    public static class Replay {
        private int stripeCount = 8;
        private int maxConcurrentStripes = 4;
        private Duration stripeLeaseDuration = Duration.ofSeconds(30);
        private Duration messageLeaseDuration = Duration.ofSeconds(30);
        private Duration indeterminateSweepInterval = Duration.ofSeconds(15);
        private int merkleCheckpointBatchSize = 50;
        private int blastRadiusWindowSize = 10;
        private double blastRadiusMaxReDltRatio = 0.25;
        private Duration blastRadiusRetentionWindow = Duration.ofMinutes(10);
        private boolean requireFourEyesApproval = true;
        private int defaultRateLimitPerSec = 50;
        private int defaultMaxRecords = 1000;
        private boolean validateTargetTopicExists = true;

        public int getStripeCount() {
            return stripeCount;
        }

        public void setStripeCount(int stripeCount) {
            this.stripeCount = stripeCount;
        }

        public int getMaxConcurrentStripes() {
            return maxConcurrentStripes;
        }

        public void setMaxConcurrentStripes(int maxConcurrentStripes) {
            this.maxConcurrentStripes = maxConcurrentStripes;
        }

        public Duration getStripeLeaseDuration() {
            return stripeLeaseDuration;
        }

        public void setStripeLeaseDuration(Duration stripeLeaseDuration) {
            this.stripeLeaseDuration = stripeLeaseDuration;
        }

        public Duration getMessageLeaseDuration() {
            return messageLeaseDuration;
        }

        public void setMessageLeaseDuration(Duration messageLeaseDuration) {
            this.messageLeaseDuration = messageLeaseDuration;
        }

        public Duration getIndeterminateSweepInterval() {
            return indeterminateSweepInterval;
        }

        public void setIndeterminateSweepInterval(Duration indeterminateSweepInterval) {
            this.indeterminateSweepInterval = indeterminateSweepInterval;
        }

        public int getMerkleCheckpointBatchSize() {
            return merkleCheckpointBatchSize;
        }

        public void setMerkleCheckpointBatchSize(int merkleCheckpointBatchSize) {
            this.merkleCheckpointBatchSize = merkleCheckpointBatchSize;
        }

        public int getBlastRadiusWindowSize() {
            return blastRadiusWindowSize;
        }

        public void setBlastRadiusWindowSize(int blastRadiusWindowSize) {
            this.blastRadiusWindowSize = blastRadiusWindowSize;
        }

        public double getBlastRadiusMaxReDltRatio() {
            return blastRadiusMaxReDltRatio;
        }

        public void setBlastRadiusMaxReDltRatio(double blastRadiusMaxReDltRatio) {
            this.blastRadiusMaxReDltRatio = blastRadiusMaxReDltRatio;
        }

        public Duration getBlastRadiusRetentionWindow() {
            return blastRadiusRetentionWindow;
        }

        public void setBlastRadiusRetentionWindow(Duration blastRadiusRetentionWindow) {
            this.blastRadiusRetentionWindow = blastRadiusRetentionWindow;
        }

        public boolean isRequireFourEyesApproval() {
            return requireFourEyesApproval;
        }

        public void setRequireFourEyesApproval(boolean requireFourEyesApproval) {
            this.requireFourEyesApproval = requireFourEyesApproval;
        }

        public int getDefaultRateLimitPerSec() {
            return defaultRateLimitPerSec;
        }

        public void setDefaultRateLimitPerSec(int defaultRateLimitPerSec) {
            this.defaultRateLimitPerSec = defaultRateLimitPerSec;
        }

        public int getDefaultMaxRecords() {
            return defaultMaxRecords;
        }

        public void setDefaultMaxRecords(int defaultMaxRecords) {
            this.defaultMaxRecords = defaultMaxRecords;
        }

        public boolean isValidateTargetTopicExists() {
            return validateTargetTopicExists;
        }

        public void setValidateTargetTopicExists(boolean validateTargetTopicExists) {
            this.validateTargetTopicExists = validateTargetTopicExists;
        }
    }

    public static class Storage {
        private boolean autoMigrate = true;
        private int maxPayloadBytes = 1_048_576;
        private int compressionThresholdBytes = 4096;
        private boolean gzipCompression = true;
        private Duration retentionTtl = Duration.ofDays(30);
        private int retentionPurgeBatchSize = 1000;
        private String cursorHmacSecret = "dlt-deck-default-dev-hmac-secret-change-in-prod-32b";

        public boolean isAutoMigrate() {
            return autoMigrate;
        }

        public void setAutoMigrate(boolean autoMigrate) {
            this.autoMigrate = autoMigrate;
        }

        public int getMaxPayloadBytes() {
            return maxPayloadBytes;
        }

        public void setMaxPayloadBytes(int maxPayloadBytes) {
            this.maxPayloadBytes = maxPayloadBytes;
        }

        public int getCompressionThresholdBytes() {
            return compressionThresholdBytes;
        }

        public void setCompressionThresholdBytes(int compressionThresholdBytes) {
            this.compressionThresholdBytes = compressionThresholdBytes;
        }

        public boolean isGzipCompression() {
            return gzipCompression;
        }

        public void setGzipCompression(boolean gzipCompression) {
            this.gzipCompression = gzipCompression;
        }

        public Duration getRetentionTtl() {
            return retentionTtl;
        }

        public void setRetentionTtl(Duration retentionTtl) {
            this.retentionTtl = retentionTtl;
        }

        public int getRetentionPurgeBatchSize() {
            return retentionPurgeBatchSize;
        }

        public void setRetentionPurgeBatchSize(int retentionPurgeBatchSize) {
            this.retentionPurgeBatchSize = retentionPurgeBatchSize;
        }

        public String getCursorHmacSecret() {
            return cursorHmacSecret;
        }

        public void setCursorHmacSecret(String cursorHmacSecret) {
            this.cursorHmacSecret = cursorHmacSecret;
        }
    }

    public static class Security {
        private boolean enabled = true;
        private boolean csrfEnabled = true;
        private @Nullable String viewerRole = "DLT_VIEWER";
        private @Nullable String operatorRole = "DLT_OPERATOR";
        private @Nullable String approverRole = "DLT_APPROVER";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isCsrfEnabled() {
            return csrfEnabled;
        }

        public void setCsrfEnabled(boolean csrfEnabled) {
            this.csrfEnabled = csrfEnabled;
        }

        public @Nullable String getViewerRole() {
            return viewerRole;
        }

        public void setViewerRole(@Nullable String viewerRole) {
            this.viewerRole = viewerRole;
        }

        public @Nullable String getOperatorRole() {
            return operatorRole;
        }

        public void setOperatorRole(@Nullable String operatorRole) {
            this.operatorRole = operatorRole;
        }

        public @Nullable String getApproverRole() {
            return approverRole;
        }

        public void setApproverRole(@Nullable String approverRole) {
            this.approverRole = approverRole;
        }
    }

    public static class Ui {
        private boolean enabled = true;
        private String title = "spring-dlt-deck";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }
    }
}

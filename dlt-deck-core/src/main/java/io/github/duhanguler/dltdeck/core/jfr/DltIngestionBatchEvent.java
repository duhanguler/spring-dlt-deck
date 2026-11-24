package io.github.duhanguler.dltdeck.core.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import org.jspecify.annotations.Nullable;

@Name("io.github.duhanguler.dltdeck.DltIngestionBatch")
@Label("DLT Ingestion Batch")
@Category({"spring-dlt-deck", "Ingestion"})
@Description("Records a persisted batch of Kafka DLT records prior to manual offset commit.")
public final class DltIngestionBatchEvent extends Event {

    @Label("DLT Topic")
    public @Nullable String dltTopic;

    @Label("Batch Size")
    public int batchSize;

    @Label("Quarantined Count")
    public int quarantinedCount;

    @Label("Re-DLT Correlated Count")
    public int reDltCount;

    @Label("Ingestion Lag")
    public long ingestionLag;
}

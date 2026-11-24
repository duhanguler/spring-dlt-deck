package io.github.duhanguler.dltdeck.core.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import org.jspecify.annotations.Nullable;

@Name("io.github.duhanguler.dltdeck.DltReplayRecord")
@Label("DLT Replay Record")
@Category({"spring-dlt-deck", "Replay"})
@Description("Records the Kafka publish and fenced TX2 outcome for a single replayed DLT record.")
public final class DltReplayRecordEvent extends Event {

    @Label("Plan ID")
    public @Nullable String planId;

    @Label("Message ID")
    public @Nullable String messageId;

    @Label("Stripe ID")
    public int stripeId;

    @Label("Target Topic")
    public @Nullable String targetTopic;

    @Label("Fencing Token")
    public long fencingToken;

    @Label("Outcome Status")
    public @Nullable String outcomeStatus;
}

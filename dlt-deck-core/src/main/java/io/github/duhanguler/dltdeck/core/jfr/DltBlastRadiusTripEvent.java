package io.github.duhanguler.dltdeck.core.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import org.jspecify.annotations.Nullable;

@Name("io.github.duhanguler.dltdeck.DltBlastRadiusTrip")
@Label("DLT Blast-Radius Circuit Trip")
@Category({"spring-dlt-deck", "Safety"})
@Description("Emitted when correlated re-DLT ratio exceeds threshold and pauses an active replay plan.")
public final class DltBlastRadiusTripEvent extends Event {

    @Label("Plan ID")
    public @Nullable String planId;

    @Label("Observed Failure Ratio")
    public double failureRatio;

    @Label("Threshold Ratio")
    public double thresholdRatio;

    @Label("Window Size")
    public int windowSize;
}

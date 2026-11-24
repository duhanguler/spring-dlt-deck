package io.github.duhanguler.dltdeck.core.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import org.jspecify.annotations.Nullable;

@Name("io.github.duhanguler.dltdeck.DltIndeterminateRecovery")
@Label("DLT Indeterminate Recovery")
@Category({"spring-dlt-deck", "Safety"})
@Description("Emitted when an expired CLAIMED lease transitions safely to INDETERMINATE or is explicitly resolved by an Approver.")
public final class DltIndeterminateRecoveryEvent extends Event {

    @Label("Message ID")
    public @Nullable String messageId;

    @Label("Previous Lease Owner")
    public @Nullable String previousLeaseOwner;

    @Label("Fencing Token")
    public long fencingToken;

    @Label("Action")
    public @Nullable String action;
}

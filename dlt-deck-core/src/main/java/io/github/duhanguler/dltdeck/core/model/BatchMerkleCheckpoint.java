package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record BatchMerkleCheckpoint(
        UUID planId,
        int batchSize,
        String merkleRoot,
        List<UUID> messageIds,
        Instant checkpointedAt) {

    public BatchMerkleCheckpoint {
        messageIds = List.copyOf(messageIds);
    }
}

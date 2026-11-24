package io.github.duhanguler.dltdeck.starter.observability;

import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

/**
 * Spring Boot Actuator endpoint exposed at {@code /actuator/dltdeck}.
 */
@Endpoint(id = "dltdeck")
public final class DltDeckActuatorEndpoint {

    private final DltDeckControlPlaneService controlPlaneService;

    public DltDeckActuatorEndpoint(DltDeckControlPlaneService controlPlaneService) {
        this.controlPlaneService = controlPlaneService;
    }

    @ReadOperation
    public Map<String, Object> summary() {
        var overview = controlPlaneService.overview();
        var audit = controlPlaneService.verifyAuditChain();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("podId", overview.podId());
        out.put("ingestionRunning", overview.ingestionRunning());
        out.put("totalIngestionLag", overview.totalIngestionLag());
        out.put("ingestionLagByTopic", overview.ingestionLagByTopic());
        out.put("subscribedTopics", overview.subscribedTopics());
        out.put("clusterCount", overview.clusterCount());
        out.put("totalMessages", overview.totalMessages());
        out.put("actionableMessages", overview.actionableMessages());
        out.put("indeterminateCount", overview.indeterminateCount());
        out.put("blockedHolCount", overview.blockedHolCount());
        out.put("pendingApprovalPlans", overview.pendingApprovalPlans());
        out.put("pausedBlastRadiusPlans", overview.pausedBlastRadiusPlans());
        out.put("auditChainIntact", audit.valid());
        out.put("auditEntriesVerified", audit.totalChecked());
        return out;
    }
}

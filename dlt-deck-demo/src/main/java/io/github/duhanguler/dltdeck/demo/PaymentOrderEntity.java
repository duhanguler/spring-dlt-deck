package io.github.duhanguler.dltdeck.demo;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Host application Hibernate 7 entity living cleanly side-by-side with {@code spring-dlt-deck}
 * (which uses Spring {@code JdbcClient} and isolated {@code dlt_deck_schema_history}).
 */
@Entity
@Table(name = "payment_orders")
public class PaymentOrderEntity {

    @Id
    private String orderId = "";

    private String customerName = "";
    private String currency = "TRY";
    private long amountCents;
    private String stage = "CREATED";
    private Instant updatedAt = Instant.now();

    protected PaymentOrderEntity() {}

    public PaymentOrderEntity(
            String orderId, String customerName, String currency, long amountCents, String stage, Instant updatedAt) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.currency = currency;
        this.amountCents = amountCents;
        this.stage = stage;
        this.updatedAt = updatedAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getCurrency() {
        return currency;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public String getStage() {
        return stage;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}

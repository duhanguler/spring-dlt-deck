package io.github.duhanguler.dltdeck.demo;

import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentOrderRepository extends JpaRepository<PaymentOrderEntity, String> {}

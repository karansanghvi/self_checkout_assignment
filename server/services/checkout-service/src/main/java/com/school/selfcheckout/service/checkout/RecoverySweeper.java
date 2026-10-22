package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.domain.CheckoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Finishes sagas that a request started but could not complete.
 *
 * A transaction stays in COMPLETING if inventory-service was down when it was
 * completed, or if checkout-service crashed between steps. The COMPLETING
 * row is the durable record that the saga is in flight, so nothing is lost:
 * this sweeper finds rows that have sat in that state for longer than any
 * live request could take, and drives them forward through the same
 * {@link CompletionSaga} a request uses. Inventory's idempotency means it does
 * not matter whether the original attempt reached inventory or not.
 *
 * Expected to find nothing in a healthy run; it logs whenever it does.
 */
@Component
public class RecoverySweeper {

    private static final Logger log = LoggerFactory.getLogger(RecoverySweeper.class);

    private static final int BATCH = 100;

    private final TransactionRepository transactionRepository;
    private final CompletionSaga saga;
    private final Duration staleAfter;

    public RecoverySweeper(TransactionRepository transactionRepository,
                           CompletionSaga saga,
                           @Value("${app.recovery.stale-after-ms:10000}") long staleAfterMs) {
        this.transactionRepository = transactionRepository;
        this.saga = saga;
        this.staleAfter = Duration.ofMillis(staleAfterMs);
    }

    @Scheduled(fixedDelayString = "${app.recovery.interval-ms:5000}")
    public void sweep() {
        List<TransactionRow> stuck;
        try {
            stuck = transactionRepository.findStuckCompleting(staleAfter, BATCH);
        } catch (DataAccessException e) {
            log.warn("Recovery sweep could not read checkout_db: {}", e.getMessage());
            return;
        }
        int recovered = 0;
        for (TransactionRow tx : stuck) {
            try {
                saga.drive(tx);
                recovered++;
            } catch (CheckoutException e) {
                log.info("Could not yet complete {}: {}", tx.id(), e.getMessage());
            }
        }
        if (!stuck.isEmpty()) {
            log.warn("Recovery sweep: {} stuck saga(s) found, {} completed", stuck.size(), recovered);
        }
    }
}

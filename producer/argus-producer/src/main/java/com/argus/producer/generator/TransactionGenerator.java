package com.argus.producer.generator;

import com.argus.producer.model.Transaction;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates realistic synthetic Transaction objects for publishing to Pub/Sub.
 * generate() is fully stateless and thread-safe.
 */

@Component
public class TransactionGenerator {
    // ── Fixed Card Pool
    // 10 cards that get reused across transactions
    private static final String[] CARD_POOL = {
        "CARD-A1B2C3D4",
        "CARD-B2C3D4E5",
        "CARD-C3D4E5F6",
        "CARD-D4E5F6A7",
        "CARD-E5F6A7B8",
        "CARD-F6A7B8C9",
        "CARD-G7H8I9J0",
        "CARD-H8I9J0K1",
        "CARD-I9J0K1L2",
        "CARD-J0K1L2M3"
    };

    private static final String[] CURRENCIES = {"CAD", "USD", "EUR", "GBP"};


    // Merchant Pool
    private static final String[] MERCHANTS = {
        "MERCH-001", "MERCH-002", "MERCH-003", "MERCH-004", "MERCH-005",
        "MERCH-006", "MERCH-007", "MERCH-008", "MERCH-009", "MERCH-010"
    };

    // Risk Zones
    // Format: {min_lat, max_lat, min_lon, max_lon}
    private static final double[][] HIGH_RISK_ZONE_1 = {{1.0, 10.0, 100.0, 115.0}};   // SE Asia
    private static final double[][] HIGH_RISK_ZONE_2 = {{5.0, 15.0, 30.0,  50.0}};    // E Africa
    private static final double[][] MEDIUM_RISK_ZONE_1 = {{35.0, 45.0, 25.0, 45.0}};  // E Europe
    private static final double[][] MEDIUM_RISK_ZONE_2 = {{-35.0, -20.0, 15.0, 35.0}};// S Africa

    public Transaction generate() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        // Roll a number 0-99:
        //   0-14  (15%) = HIGH_RISK zone 1 (SE Asia)
        //   15-22  (8%) = HIGH_RISK zone 2 (E Africa)
        //   23-26  (4%) = MEDIUM_RISK zone 1 (E Europe)
        //   27-29  (3%) = MEDIUM_RISK zone 2 (S Africa)
        //   30-99 (70%) = anywhere on Earth (likely STANDARD)

        double lat, lon;
        double zoneRoll = rng.nextInt(100);

        if (zoneRoll < 15) {
            // HIGH_RISK zone 1: Southeast Asia
            lat = rng.nextDouble(1.0, 10.0);
            lon = rng.nextDouble(100.0, 115.0);
        } else if (zoneRoll < 23) {
            // HIGH_RISK zone 2: East Africa
            lat = rng.nextDouble(5.0, 15.0);
            lon = rng.nextDouble(30.0, 50.0);
        } else if (zoneRoll < 27) {
            // MEDIUM_RISK zone 1: Eastern Europe
            lat = rng.nextDouble(35.0, 45.0);
            lon = rng.nextDouble(25.0, 45.0);
        } else if (zoneRoll < 30) {
            // MEDIUM_RISK zone 2: Southern Africa
            lat = rng.nextDouble(-35.0, -20.0);
            lon = rng.nextDouble(15.0, 35.0);
        } else {
            // STANDARD: random anywhere on Earth
            lat = rng.nextDouble(-90.0, 90.0);
            lon = rng.nextDouble(-180.0, 180.0);
        }

        // Generate the amounbt
        BigDecimal amount = BigDecimal.valueOf(rng.nextDouble(1.0, 5000.0)).setScale(2, RoundingMode.HALF_UP);

        // Build/return the transaction
        return Transaction.builder()
            .txId(UUID.randomUUID().toString())
            .cardId(CARD_POOL[rng.nextInt(CARD_POOL.length)])
            .amount(amount)
            .currency(CURRENCIES[rng.nextInt(CURRENCIES.length)])
            .merchantId(MERCHANTS[rng.nextInt(MERCHANTS.length)])
            .lat(lat)
            .lon(lon)
            .timestamp(Instant.now().toString())
            .build();
    }
}

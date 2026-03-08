package com.argus.producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Represents a single financial transaction published to Pub/Sub.
 * tx_id → UUID string
 * card_id → matches CARD-[A-Z0-9]{8} regex
 * amount → decimal, > 0, ≤ 1,000,000
 * currency → one of: CAD, USD, EUR, GBP
 * merchant_id → non-empty string
 * lat → -90.0 to 90.0
 * lon → -180.0 to 180.0
 * timestamp → ISO 8601 UTC string
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    @JsonProperty("tx_id")
    private String txId;

    @JsonProperty("card_id")
    private String cardId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("merchant_id")
    private String merchantId;

    @JsonProperty("lat")
    private double lat;

    @JsonProperty("lon")
    private double lon;

    @JsonProperty("timestamp")
    private String timestamp;
}

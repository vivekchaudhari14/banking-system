package com.banking.paymentservice.dto;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PaymentOrderResponse {

    String paymentId;
    String rozorpayOrederId;
    BigDecimal amount;
    String currency;
    String status;
    String razorpayKeyId;


}

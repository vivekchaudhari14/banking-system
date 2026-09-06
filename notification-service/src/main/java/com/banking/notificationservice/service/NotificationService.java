package com.banking.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(
            topics = "transaction.otp.generated",
            groupId = "notification-service"
    )
    public void consumeOTPGenerated(
            @Payload Map<String, Object> payload) {

        try {

            String accountNumber =
                    String.valueOf(payload.get("accountNumber"));

            String otp =
                    String.valueOf(payload.get("otp"));

            String transactionId =
                    String.valueOf(payload.get("transactionId"));

            String amount =
                    String.valueOf(payload.get("amount"));

            String reason =
                    String.valueOf(payload.get("reason"));

            sendAlert(
                    accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. " +
                                    "Reason: %s. " +
                                    "A transaction of %s is pending verification. " +
                                    "Your OTP is: %s. Valid for 5 minutes. " +
                                    "If this wasn't you, ignore this message.",
                            reason,
                            amount,
                            otp
                    )
            );

            log.info(
                    "OTP notification sent for transactionId={}",
                    transactionId
            );

        } catch (Exception e) {

            log.error(
                    "Error sending OTP notification",
                    e
            );
        }
    }


    @KafkaListener(
            topics = "transaction.completed",
            groupId = "notification-service"
    )
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {

        try {

            String senderAccountNumber =
                    String.valueOf(payload.get("senderAccountNumber"));

            String receiverAccountNumber =
                    String.valueOf(payload.get("receiverAccountNumber"));

            String amount =
                    String.valueOf(payload.get("amount"));

            String transactionId =
                    String.valueOf(payload.get("transactionId"));


            // DEBIT ALERT
            sendAlert(
                    senderAccountNumber,
                    "DEBIT ALERT",
                    String.format(
                            "%s debited from account %s",
                            amount,
                            senderAccountNumber
                    )
            );


            // CREDIT ALERT
            sendAlert(
                    receiverAccountNumber,
                    "CREDIT ALERT",
                    String.format(
                            "%s credited to account %s",
                            amount,
                            receiverAccountNumber
                    )
            );

            log.info(
                    "Transaction notification sent for transactionId={}",
                    transactionId
            );

        } catch (Exception e) {

            log.error(
                    "Error sending transaction notification",
                    e
            );
        }
    }


    @KafkaListener(
            topics = "fraud.detected",
            groupId = "notification-service"
    )
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload) {

        try {

            String accountNumber =
                    String.valueOf(payload.get("accountNumber"));

            String reason =
                    String.valueOf(payload.get("reason"));

            sendAlert(
                    accountNumber,
                    "SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked. " +
                                    "Reason: %s. " +
                                    "Please contact your bank immediately.",
                            accountNumber,
                            reason
                    )
            );

        } catch (Exception e) {

            log.error(
                    "Error sending fraud alert",
                    e
            );
        }
    }


    @KafkaListener(
            topics = "transaction.refunded",
            groupId = "notification-service"
    )
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload) {

        try {

            String senderAccountNumber =
                    String.valueOf(payload.get("senderAccountNumber"));

            String amount =
                    String.valueOf(payload.get("amount"));

            String reason =
                    String.valueOf(payload.get("reason"));

            sendAlert(
                    senderAccountNumber,
                    "REFUND PROCESSED",
                    String.format(
                            "Your transaction has been refunded. " +
                                    "Account: %s. " +
                                    "Reason: %s. " +
                                    "%s has been refunded to account %s.",
                            senderAccountNumber,
                            reason,
                            amount,
                            senderAccountNumber
                    )
            );

        } catch (Exception e) {

            log.error(
                    "Error sending refund notification",
                    e
            );
        }
    }


    @KafkaListener(
            topics = "payment.completed",
            groupId = "notification-service"
    )
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload) {

        try {

            String accountNumber =
                    String.valueOf(payload.get("accountNumber"));

            String amount =
                    String.valueOf(payload.get("amount"));

            String razorpayPaymentId =
                    String.valueOf(payload.get("razorpayPaymentId"));

            sendAlert(
                    accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                            "Payment of %s completed successfully. " +
                                    "Razorpay ID: %s.",
                            amount,
                            razorpayPaymentId
                    )
            );

        } catch (Exception e) {

            log.error(
                    "Error sending payment notification",
                    e
            );
        }
    }


    @KafkaListener(
            topics = "payment.failed",
            groupId = "notification-service"
    )
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload) {

        try {

            String accountNumber =
                    String.valueOf(payload.get("accountNumber"));

            String amount =
                    String.valueOf(payload.get("amount"));

            sendAlert(
                    accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of %s could not be processed. " +
                                    "Please try again or contact support.",
                            amount
                    )
            );

        } catch (Exception e) {

            log.error(
                    "Error sending payment failure notification",
                    e
            );
        }
    }


    private void sendAlert(
            String accountNumber,
            String subject,
            String message) {

        log.info("----------------------------------");
        log.info("Account number: {}", accountNumber);
        log.info("Subject: {}", subject);
        log.info("Message: {}", message);
        log.info("----------------------------------");
    }
}
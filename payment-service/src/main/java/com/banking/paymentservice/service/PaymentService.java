package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.entity.PaymentOutboxEvent;
import com.banking.paymentservice.repository.PaymentOutboxEventRepository;
import com.razorpay.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.EventStatus;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;

import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentOutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;


    @Value("${razorpay.key.id}")
    private String keyId;

    @Value("${razorpay.key.secret}")
    private String keySecret;

    @Value("${razorpay.webhook.secret}")
    private String webhookSecret;


    /*
     * Kafka topics
     */
    private static final String PAYMENT_COMPLETED_TOPIC =
            "payment.completed";

    private static final String PAYMENT_FAILED_TOPIC =
            "payment.failed";


    // =========================================================
    // CREATE RAZORPAY ORDER
    // =========================================================

    public PaymentOrderResponse createPaymentOrder(
            CreatePaymentRequest request) throws RazorpayException {

        log.info(
                "Creating payment order for account: {} amount: {}",
                request.getAccountNumber(),
                request.getAmount()
        );


        // -----------------------------------------------------
        // 1. Create Razorpay client
        // -----------------------------------------------------

        RazorpayClient razorpayClient =
                new RazorpayClient(
                        keyId,
                        keySecret
                );


        // -----------------------------------------------------
        // 2. Convert amount into paise
        // -----------------------------------------------------

        long convertedAmount =
                request.getAmount()
                        .movePointRight(2)
                        .longValueExact();


        // -----------------------------------------------------
        // 3. Create Razorpay order request
        // -----------------------------------------------------

        JSONObject orderRequest =
                new JSONObject();

        orderRequest.put(
                "amount",
                convertedAmount
        );

        orderRequest.put(
                "currency",
                "INR"
        );

        orderRequest.put(
                "receipt",
                "rcpt"
                        + System.currentTimeMillis()
                        + UUID.randomUUID()
                        .toString()
                        .replace("-", "")
                        .substring(0, 10)
        );


        // -----------------------------------------------------
        // 4. Create order in Razorpay
        // -----------------------------------------------------

        Order razorpayOrder =
                razorpayClient.orders.create(
                        orderRequest
                );


        String razorpayOrderId =
                razorpayOrder
                        .get("id")
                        .toString();


        log.info(
                "Razorpay order created: {}",
                razorpayOrderId
        );


        // -----------------------------------------------------
        // 5. Save payment in database
        // -----------------------------------------------------

        Payment payment =
                new Payment();

        payment.setRazorpayOrderId(
                razorpayOrderId
        );

        payment.setAccountNumber(
                request.getAccountNumber()
        );

        payment.setAmount(
                request.getAmount()
        );

        payment.setCurrency(
                "INR"
        );

        payment.setStatus(
                PaymentStatus.CREATED
        );

        /*
         * Kafka event अजून publish झालेला नाही.
         */
        payment.setEventStatus(
                EventStatus.PENDING
        );

        payment.setDescription(
                request.getDescription()
        );


        Payment savedPayment =
                paymentRepository.save(
                        payment
                );


        log.info(
                "Payment record saved successfully. Payment ID: {}",
                savedPayment.getId()
        );


        // -----------------------------------------------------
        // 6. Return response
        // -----------------------------------------------------

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrderId,
                request.getAmount(),
                "INR",
                "CREATED",
                keyId
        );
    }


    // =========================================================
    // RAZORPAY WEBHOOK
    // =========================================================

    @Transactional
    public void handleWebhook(
            String rawPayload,
            String signature) {

        try {

            // 1. Verify Razorpay signature
            Utils.verifyWebhookSignature(
                    rawPayload,
                    signature,
                    webhookSecret
            );

            log.info("Razorpay webhook signature verified successfully");


            // 2. Convert JSON → Map
            Map<String, Object> payload =
                    objectMapper.readValue(
                            rawPayload,
                            new TypeReference<Map<String, Object>>() {}
                    );


            // 3. Get event
            Object eventObject =
                    payload.get("event");

            if (eventObject == null) {

                log.warn("Webhook event is missing");

                return;
            }

            String event =
                    eventObject.toString();


            // 4. Process event
            if ("payment.captured".equals(event)) {

                handlePaymentSuccess(payload);

            } else if ("payment.failed".equals(event)) {

                handlePaymentFailure(payload);

            } else {

                log.info(
                        "Ignoring unsupported Razorpay event: {}",
                        event
                );
            }


        } catch (RazorpayException e) {

            log.error(
                    "Invalid Razorpay webhook signature",
                    e
            );

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid webhook signature"
            );


        } catch (Exception e) {

            log.error(
                    "Error processing Razorpay webhook",
                    e
            );

            throw new RuntimeException(
                    "Webhook processing failed",
                    e
            );
        }
    }


    // =========================================================
    // PAYMENT SUCCESS
    // =========================================================

    private void handlePaymentSuccess(
            Map<String, Object> payload) {

        try {

            // -------------------------------------------------
            // 1. Extract payment data
            // -------------------------------------------------

            Map<String, Object> paymentData =
                    extractPaymentData(payload);


            Object orderIdObject =
                    paymentData.get("order_id");

            Object paymentIdObject =
                    paymentData.get("id");


            if (orderIdObject == null) {

                throw new RuntimeException(
                        "order_id missing from Razorpay webhook"
                );
            }


            if (paymentIdObject == null) {

                throw new RuntimeException(
                        "payment id missing from Razorpay webhook"
                );
            }


            String orderId =
                    orderIdObject.toString();


            String paymentId =
                    paymentIdObject.toString();


            log.info(
                    "Processing successful payment. " +
                            "Order ID: {}, Payment ID: {}",
                    orderId,
                    paymentId
            );


            // -------------------------------------------------
            // 2. Find payment
            // -------------------------------------------------

            Payment payment =
                    paymentRepository
                            .findByRazorpayOrderId(orderId)
                            .orElseThrow(
                                    () -> new RuntimeException(
                                            "Payment not found for Razorpay order: "
                                                    + orderId
                                    )
                            );


            // -------------------------------------------------
            // 3. IDEMPOTENCY CHECK
            // -------------------------------------------------

            /*
             * जर payment COMPLETED आहे
             * आणि Kafka event successfully published आहे,
             * तर हा duplicate webhook आहे.
             */




            // -------------------------------------------------
            // 4. Update payment
            // -------------------------------------------------

            payment.setRazorpayPaymentId(
                    paymentId
            );

            payment.setStatus(
                    PaymentStatus.COMPLETED
            );

            /*
             * Kafka event publishing सुरू करणार आहोत.
             */
            payment.setEventStatus(
                    EventStatus.PENDING
            );


            // -------------------------------------------------
            // 5. Save COMPLETED payment
            // -------------------------------------------------

            paymentRepository.save(
                    payment
            );


            log.info(
                    "Payment marked COMPLETED. Payment ID: {}",
                    payment.getId()
            );


            // -------------------------------------------------
            // 6. Create Kafka event
            // -------------------------------------------------

            Map<String, Object> event =
                    new HashMap<>();


            event.put(
                    "paymentId",
                    payment.getId()
            );


            event.put(
                    "accountNumber",
                    payment.getAccountNumber()
            );


            event.put(
                    "amount",
                    payment.getAmount()
            );


            event.put(
                    "razorpayPaymentId",
                    paymentId
            );


            event.put(
                    "razorpayOrderId",
                    orderId
            );


            // -------------------------------------------------
            // 7. Publish Kafka event
            // -------------------------------------------------

            PaymentOutboxEvent outboxEvent =
                    new PaymentOutboxEvent();

            outboxEvent.setEventType(
                    PAYMENT_COMPLETED_TOPIC
            );

            outboxEvent.setAggregateId(
                    payment.getId()
            );

            outboxEvent.setPayload(
                    objectMapper.writeValueAsString(event)
            );

            outboxEvent.setStatus(
                    EventStatus.PENDING
            );

            outboxEventRepository.save(outboxEvent);

            log.info(
                    "payment.completed outbox event created. Payment ID: {}",
                    payment.getId()
            );


        } catch (Exception e) {

            log.error(
                    "Error handling payment success: {}",
                    e.getMessage(),
                    e
            );

            throw new RuntimeException("Payment success processing failed", e);
        }
    }


    // =========================================================
    // PAYMENT FAILURE
    // =========================================================

    private void handlePaymentFailure(
            Map<String, Object> payload) {

        try {

            // -------------------------------------------------
            // 1. Extract payment data
            // -------------------------------------------------

            Map<String, Object> paymentData =
                    extractPaymentData(payload);


            Object orderIdObject =
                    paymentData.get("order_id");


            if (orderIdObject == null) {

                throw new RuntimeException(
                        "order_id missing from Razorpay failure webhook"
                );
            }


            String orderId =
                    orderIdObject.toString();


            // -------------------------------------------------
            // 2. Find payment
            // -------------------------------------------------

            Payment payment =
                    paymentRepository
                            .findByRazorpayOrderId(orderId)
                            .orElseThrow(
                                    () -> new RuntimeException(
                                            "Payment not found for Razorpay order: "
                                                    + orderId
                                    )
                            );


            // -------------------------------------------------
            // 3. Idempotency check
            // -------------------------------------------------


            // -------------------------------------------------
            // 4. Update payment
            // -------------------------------------------------

            payment.setStatus(
                    PaymentStatus.FAILED
            );


            payment.setFailureReason(
                    "Payment Failed Via Razorpay"
            );


            payment.setEventStatus(
                    EventStatus.PENDING
            );


            // -------------------------------------------------
            // 5. Save payment
            // -------------------------------------------------

            paymentRepository.save(
                    payment
            );


            log.info(
                    "Payment marked FAILED. Payment ID: {}",
                    payment.getId()
            );


            // -------------------------------------------------
            // 6. Create Kafka event
            // -------------------------------------------------

            Map<String, Object> event =
                    new HashMap<>();


            event.put(
                    "paymentId",
                    payment.getId()
            );


            event.put(
                    "accountNumber",
                    payment.getAccountNumber()
            );


            event.put(
                    "amount",
                    payment.getAmount()
            );


            event.put(
                    "reason",
                    "Payment Failed Via Razorpay"
            );


            // -------------------------------------------------
            // 7. Publish payment.failed
            // -------------------------------------------------

            PaymentOutboxEvent outboxEvent =
                    new PaymentOutboxEvent();

            outboxEvent.setEventType(
                    PAYMENT_FAILED_TOPIC
            );

            outboxEvent.setAggregateId(
                    payment.getId()
            );

            outboxEvent.setPayload(
                    objectMapper.writeValueAsString(event)
            );

            outboxEvent.setStatus(
                    EventStatus.PENDING
            );

            outboxEventRepository.save(outboxEvent);

            log.info(
                    "payment.failed outbox event created. Payment ID: {}",
                    payment.getId()
            );


        } catch (Exception e) {

            log.error(
                    "Error handling payment failure: {}",
                    e.getMessage(),
                    e
            );

            throw new RuntimeException("Payment failure processing failed", e);
        }
    }


    // =========================================================
    // EXTRACT PAYMENT DATA
    // =========================================================

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractPaymentData(
            Map<String, Object> payload) {

        Object payloadObject =
                payload.get("payload");

        if (!(payloadObject instanceof Map)) {

            throw new RuntimeException(
                    "Invalid Razorpay webhook: payload missing"
            );
        }

        Map<String, Object> payloadData =
                (Map<String, Object>) payloadObject;


        Object paymentObject =
                payloadData.get("payment");

        if (!(paymentObject instanceof Map)) {

            throw new RuntimeException(
                    "Invalid Razorpay webhook: payment missing"
            );
        }

        Map<String, Object> paymentWrapper =
                (Map<String, Object>) paymentObject;


        Object entityObject =
                paymentWrapper.get("entity");

        if (!(entityObject instanceof Map)) {

            throw new RuntimeException(
                    "Invalid Razorpay webhook: entity missing"
            );
        }

        return (Map<String, Object>) entityObject;
    }
}
package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor

public class PaymentService {

    /**
     *
     */
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key.id}")
    private String keyId;

    @Value("${razorpay.key.secret}")
    private String keySecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";

    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";



    /*

        Create Razorpay payment order

        Flow:
            1.Create Order in razorpay
            2. save payment record in DB
            3. return order details to frontend
            4. Fronted show RazorPay Checkout
            5. User pays
            6. Razorpay calls webhook
     */

    public PaymentOrderResponse createPaymentOrder(
            CreatePaymentRequest request ) throws RazorpayException {
        log.info("Creating payment order for account: {} amount: {} ",
                request.getAccountNumber(), request.getAmount());
        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        //converted Amount
        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "INR");
        orderRequest.put("receipt","rcpt"  + System.currentTimeMillis()
                +UUID.randomUUID().toString().replace("-","")
                .substring(0,10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Payment order created: {}", razorpayOrder.get("id").toString());

        // saved payment record

        Payment payment = new Payment();


        // Don't set payment.id manually

        payment.setRazorpayOrderId(
                razorpayOrder.get("id").toString()
        );

        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                "INR",
                "CREATED",
                keyId
        );

    }

    public void handleWebhook(Map<String,Object> payload) {
        log.info("Received Razorpay Webhook: {}", payload.get("event"));

        String event = payload.get("event").toString();

        if ("payment.captured".equals(event)) {
            handlePaymentSuccess(payload);
        } else if ("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String,Object> payload) {
        try {

            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = paymentData.get("order_id").toString();
            String paymentId = paymentData.get("id").toString();

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()-> new RuntimeException("Payment Not Found order"+orderId));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            // publish payment completed event
            Map<String,Object> event = new HashMap<>();
            event.put("paymentId", paymentId);
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId(),event);
            log.info("Payment Completed: {}", payment.getId());


        }catch (Exception e) {
            log.error("Error handling payment success: {}", e.getMessage());

        }
    }

    private void handlePaymentFailure(Map<String,Object> payload) {
        try {

            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = paymentData.get("order_id").toString();

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()-> new RuntimeException("Payment Not Found order"+orderId));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment Failed Via Razorpay");
            paymentRepository.save(payment);

            // publish payment completed event

            Map<String,Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", "Payment Failed Via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(),event);

            log.info("Payment Failed: {}", payment.getId());

        }catch (Exception e) {

            log.error("Error handling payment failure: {}", e.getMessage());

        }
    }

    private Map<String, Object> extractPaymentData(
            Map<String, Object> payload) {

        Map<String, Object> payloadData =
                (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper =
                (Map<String, Object>) payloadData.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }

}

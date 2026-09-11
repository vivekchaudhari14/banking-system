**Digital Banking System | How Banks Detect Fraud | Spring Boot + Kafka + Redis + SAGA**

# 🏦 How Banks Detect Fraud | Digital Banking System

A **Digital Banking & Fraud Detection System** built using **Spring Boot, Microservices, Apache Kafka, Redis, MySQL, SAGA Pattern, Outbox Pattern, Razorpay and Spring Cloud Gateway**.

The project demonstrates how a modern banking/payment system can process transactions asynchronously while detecting suspicious activity, handling OTP verification, compensating failed transactions, and sending notifications.

---

## 🚀 Project Overview

This project simulates a digital banking platform where users can:

- Create and manage bank accounts
- Check account balances
- Transfer money between accounts
- Detect suspicious transactions
- Trigger OTP verification for suspicious transactions
- Complete or reject transactions
- Automatically compensate failed transactions
- Block accounts involved in confirmed fraud
- Send fraud/transaction notifications
- Process payments using Razorpay
- Communicate between microservices using Apache Kafka

The system follows a **distributed microservices architecture** with an event-driven communication model.

---

# 🏗️ Architecture

```text
                         ┌──────────────────────┐
                         │      Client/UI       │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    API Gateway       │
                         │       :8080          │
                         │ Spring Cloud Gateway │
                         └──────────┬───────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
                 ▼                  ▼                  ▼
        ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
        │ Account Service│ │ Transaction    │ │ Payment Service│
        │     :8081      │ │    Service     │ │     :8083      │
        └───────┬────────┘ │     :8082      │ └───────┬────────┘
                │          └────────┬───────┘         │
                │                   │                 │
                │                   ▼                 │
                │          ┌─────────────────┐        │
                │          │ Fraud Detection  │        │
                │          │     :8084        │        │
                │          └────────┬────────┘        │
                │                   │                 │
                │                   ▼                 │
                │          ┌─────────────────┐        │
                │          │ Notification    │        │
                │          │    Service      │        │
                │          │      :8085       │        │
                │          └─────────────────┘        │
                │                                     │
                └──────────────┬──────────────────────┘
                               │
              ┌────────────────┼─────────────────┐
              │                │                 │
              ▼                ▼                 ▼
        ┌──────────┐      ┌──────────┐      ┌────────────┐
        │  MySQL   │      │  Redis   │      │   Kafka    │
        │ Database │      │  Cache   │      │ Event Bus  │
        └──────────┘      └──────────┘      └────────────┘

                         ┌────────────┐
                         │  Razorpay  │
                         │ Payment GW │
                         └────────────┘
🧩 Microservices
Service	Port	Responsibility
API Gateway	8080	Single entry point, routing and rate limiting
Account Service	8081	Account creation, balance management and blocking
Transaction Service	8082	Money transfer orchestration and transaction state
Payment Service	8083	Razorpay order/payment processing
Fraud Detection Service	8084	Fraud/risk analysis
Notification Service	8085	Transaction and fraud notifications

🛠️ Technology Stack
Backend
Java
Spring Boot
Spring Web
Spring Data JPA
Spring Cloud Gateway
Spring Cloud OpenFeign
Spring Validation
Messaging
Apache Kafka
Kafka Producer
Kafka Consumer
Event-driven architecture
Database
MySQL
Spring Data JPA / Hibernate
Caching / Fraud Detection
Redis
Redis counters
Redis TTL
Velocity detection
Transaction history tracking
Payment
Razorpay API
Razorpay Webhooks
Webhook signature verification
Architecture Patterns
Microservices
SAGA Pattern
Event-Driven Architecture
Transactional Outbox Pattern
Idempotency
Compensation
Eventual Consistency


🔄 Transaction Flow

A normal transaction follows this flow:

Client
  │
  ▼
API Gateway
  │
  ▼
Transaction Service
  │
  ├── Validate transaction
  │
  ├── Check Idempotency-Key
  │
  ├── Deduct sender balance
  │
  └── Publish transaction.initiated
              │
              ▼
      Fraud Detection Service
              │
              ├── Amount Analysis
              ├── Velocity Analysis
              ├── Historical Analysis
              └── Balance Analysis
              │
        ┌─────┴──────┐
        │            │
     CLEAN        SUSPICIOUS
        │            │
        ▼            ▼
 fraud.check.clean  verification.required
        │            │
        ▼            ▼
Transaction       OTP Generated
Completed             │
        │              ▼
        │        User verifies OTP
        │          │          │
        │       SUCCESS      FAILURE
        │          │          │
        ▼          ▼          ▼
Receiver       Complete    Refund + Block
Credit         Transaction    Account


🔍 Fraud Detection Flow

The Fraud Detection Service evaluates transactions using multiple signals.

1. Amount Analysis

The system maintains historical transaction information in Redis.

Example:

Historical Average = ₹10,000

Current Transaction = ₹80,000

If current amount > configured threshold
        ↓
Transaction becomes suspicious
2. Transaction Velocity

The system tracks transaction frequency using Redis.

Example:

User performs:

Transaction 1
Transaction 2
Transaction 3
Transaction 4
Transaction 5

If the number of transactions crosses the configured threshold within the time window:

Velocity exceeded
        ↓
Suspicious Transaction

Redis TTL is used for the velocity window.

3. Balance Percentage Check

The fraud service also evaluates the transaction amount against the sender's account balance.

Transaction Amount
        /
Sender Balance
        ↓
Risk Percentage

If the percentage exceeds the configured limit, the transaction can be marked suspicious.

4. Historical Transaction Pattern

The system maintains transaction count and total transaction amount in Redis.

This allows the service to calculate a historical average and identify unusually large transactions.

🔐 OTP Verification

Suspicious transactions require additional verification.

Fraud Detection
      │
      ▼
verification.required
      │
      ▼
Transaction Service
      │
      ▼
Generate OTP
      │
      ├── Store OTP in Redis
      │
      └── Publish transaction.otp.generated
                   │
                   ▼
             Notification

The OTP is stored temporarily using Redis TTL.

❌ Wrong OTP Flow

The system tracks OTP verification attempts.

Wrong OTP
   │
   ▼
Increment Attempt Counter
   │
   ├── Attempts < 3
   │       │
   │       └── Reject verification
   │
   └── Attempts >= 3
           │
           ▼
      Compensation
           │
           ▼
      Refund Balance
           │
           ▼
      Block Account
           │
           ▼
      fraud.detected
           │
           ▼
      Notification


💰 SAGA Transaction Pattern

The project demonstrates a distributed SAGA-style transaction flow.

A transaction can involve multiple services:

Transaction Service
       │
       ▼
Debit Account
       │
       ▼
Fraud Detection
       │
       ├── CLEAN ────────────────► Complete
       │
       └── FRAUD
             │
             ▼
          COMPENSATE
             │
             ▼
        Refund Balance
             │
             ▼
        Block Account

Since distributed services cannot share one database transaction, compensation is used when a later operation fails.

📦 Transactional Outbox Pattern

The project uses the Transactional Outbox Pattern for important asynchronous events.

Instead of directly depending on Kafka after updating the database:

Database Transaction
       │
       ├── Update Business Data
       │
       └── Insert Outbox Event
                │
                ▼
          Outbox Publisher
                │
                ▼
              Kafka

This helps reduce the risk of:

Database updated successfully
          +
Kafka event failed

📨 Kafka Events

The system uses Kafka topics for asynchronous communication.

Examples include:

transaction.initiated
verification.required
transaction.otp.generated
fraud.check.clean
fraud.detected
transaction.completed
transaction.refunded
payment.completed
payment.failed

🧠 Event-Driven Architecture

Services communicate through events where asynchronous processing is appropriate.

Example:

Transaction Service
       │
       │ transaction.initiated
       ▼
Kafka
       │
       ▼
Fraud Detection Service
       │
       │ fraud.check.clean
       ▼
Kafka
       │
       ▼
Transaction Service

This reduces direct coupling between services.

🗄️ Database Design

Each business service is responsible for its own data.

Account Service
Account
 ├── id
 ├── accountNumber
 ├── accountHolderName
 ├── balance
 ├── status
 ├── createdAt
 └── updatedAt

Account status can include:

ACTIVE
BLOCKED
Transaction Service

Transaction information includes:

Transaction
 ├── transactionId
 ├── senderAccount
 ├── receiverAccount
 ├── amount
 ├── status
 ├── idempotencyKey
 ├── createdAt
 └── updatedAt

Possible transaction states include:

PENDING
PROCESSING
PENDING_VERIFICATION
COMPLETED
FAILED
FLAGGED
Payment Service

Payment information includes Razorpay-related data such as:

Payment
 ├── paymentId
 ├── orderId
 ├── amount
 ├── currency
 ├── status
 └── eventStatus

🔑 Idempotency

Transaction requests support an Idempotency-Key.

Example:

POST /transactions
Idempotency-Key: abc-123

If the client sends the same request again:

First Request
     │
     ▼
Transaction Created
     │
     ▼
Idempotency Key Stored

Second Request
     │
     ▼
Same Idempotency Key
     │
     ▼
Return Existing Transaction

This helps prevent duplicate money transfers caused by:

Double-clicks
Network retries
Client retries
Request timeouts

💳 Razorpay Integration

The Payment Service integrates with Razorpay.

Basic flow:

Client
  │
  ▼
Payment Service
  │
  ▼
Create Razorpay Order
  │
  ▼
Razorpay
  │
  ▼
Payment
  │
  ▼
Razorpay Webhook
  │
  ▼
Payment Service
  │
  ├── Verify Signature
  │
  ├── Update Payment
  │
  └── Publish Payment Event

Webhook signature verification is used to validate incoming Razorpay webhook requests.

🚦 API Gateway

Spring Cloud Gateway acts as the public entry point.

Example routing:

/api/accounts/**      → Account Service
/api/transactions/**  → Transaction Service
/api/payments/**      → Payment Service

The gateway also contains a rate-limiting configuration using Redis.

🧯 Rate Limiting

Redis is used by the API Gateway for rate limiting.

Conceptually:

Client
  │
  ▼
API Gateway
  │
  ▼
Rate Limiter
  │
  ├── Allowed ───────► Service
  │
  └── Limit exceeded
             │
             ▼
        HTTP 429

This helps protect backend services from excessive requests.

📡 Service-to-Service Communication

The project uses OpenFeign for synchronous communication between services.

Example:

Transaction Service
        │
        ▼
Account Service

This is used for operations such as:

Check Account
Deduct Balance
Credit Balance
Refund Balance

Kafka is used when asynchronous/event-driven communication is more appropriate.

🔄 Synchronous vs Asynchronous Communication

Communication	Technology	Example
Synchronous	OpenFeign	Deduct account balance
Asynchronous	Kafka	Fraud result
Asynchronous	Kafka	OTP event
Asynchronous	Kafka	Transaction completed
Asynchronous	Kafka	Notifications

📊 Redis Usage

Redis is used for several purposes.

Fraud Detection
Transaction Count
Transaction Total
Velocity Counter
OTP
OTP
OTP Expiration
OTP Attempts
Gateway
Rate Limiting

🛡️ Reliability Features

The project demonstrates several reliability concepts:

Idempotency
Database unique constraints
Kafka consumers
Kafka retry handling
Dead Letter Topic configuration
Transactional Outbox
Compensation
Redis TTL
Webhook signature validation
Account locking during balance updates

⚠️ Current Known Limitations / Mismatches

This is a learning/portfolio project and is not production-ready banking software.

The following areas should be improved before describing it as production-grade.

1. Kafka Consumer Group Naming

Transaction Service has different group identifiers between YAML configuration and listener annotations.

The listener-level groupId overrides the configured consumer group.

Recommended approach:

Use one consistent consumer group per logical consumer.
2. Transaction Outbox Event Type

The transaction outbox stores:

TRANSACTION_INITIATED

while the publisher sends to:

transaction.initiated

The event type/topic naming should be standardized.

3. Razorpay Configuration Naming

The Payment Service expects properties such as:

razorpay.key.id
razorpay.key.secret
razorpay.webhook.secret

while the configuration currently uses:

razorpay.key-id
razorpay.key-secret
razorpay.webhook-secret

These property names must be aligned.

4. ObjectMapper Package Mismatch

Most services use:

com.fasterxml.jackson.databind.ObjectMapper

but one transaction component uses:

tools.jackson.databind.ObjectMapper

The project should standardize on the Jackson ObjectMapper used by Spring Boot.

5. Fraud Balance Calculation

The transaction service deducts the sender's balance before fraud detection.

Therefore, when Fraud Detection retrieves the balance, it can see the post-debit balance.

For example:

Initial Balance = ₹100,000

Transfer = ₹80,000

Balance after debit = ₹20,000

Fraud Detection may compare:

₹80,000 vs ₹20,000

instead of:

₹80,000 vs ₹100,000

The fraud rule should be redesigned if it is intended to use the pre-transaction balance.

6. Compensation and Remote Calls

Compensation currently combines:

Database Transaction
+
Remote Account Service Call
+
Kafka Publishing

These cannot be rolled back as one atomic transaction.

A more robust implementation would use:

Saga State Machine
+
Outbox
+
Retry
+
Compensation Events
7. Some Kafka Events Bypass Outbox

Important events such as some fraud/OTP/compensation events are published directly through Kafka.

For stronger reliability, important business events should consistently use the Outbox Pattern.

8. OTP Publishing Reliability

OTP state can be stored successfully while the Kafka event fails.

That can result in:

Transaction = PENDING_VERIFICATION

OTP Notification = Not Delivered

An outbox event would provide a more reliable solution.

9. Payment Webhook Idempotency

Razorpay can retry webhook delivery.

Webhook processing should therefore be idempotent.

Recommended:

Unique webhook/event ID
+
Database uniqueness
+
State transition validation
10. Payment Order Idempotency

Payment order creation should also support an idempotency mechanism to prevent duplicate payment orders when clients retry.

11. Notification Idempotency

The Notification Service currently does not persist processed event IDs.

In a real distributed system, duplicate Kafka delivery could result in duplicate notifications.

Recommended:

eventId + consumerName

with a unique database constraint.

12. Notification Implementation

Notifications are currently simulated/logged.

Production implementation could integrate:

SMS Provider
Email Provider
Push Notification Provider
13. Fraud Historical Data

Suspicious transactions should generally not contaminate the trusted historical baseline.

For example:

Normal Transactions
        ↓
Historical Average
        ↓
Fraud Model

Suspicious transactions should be handled carefully before being added to the baseline.

14. Redis Atomicity

Some Redis fraud counters use separate operations.

Concurrent transactions can cause race conditions.

For stronger consistency, consider:

Redis Lua Script
Redis Transaction
Redis Hash
Atomic INCR operations

depending on the use case.

15. Account Credit Concurrency

Credit/refund idempotency checks should be carefully ordered with account locking.

Recommended pattern:

Lock Account
     ↓
Check Idempotency
     ↓
Update Balance
     ↓
Save Processed Operation

inside one database transaction.

16. Transaction Status Semantics

The transaction may be marked COMPLETED before the receiver's account credit is actually confirmed.

For stronger state semantics, consider states such as:

SENDER_DEBITED
FRAUD_CHECKED
RECEIVER_CREDIT_PENDING
COMPLETED


📁 Suggested Project Structure
digital-banking-system/
│
├── api-gateway/
│
├── account-service/
│
├── transaction-service/
│
├── payment-service/
│
├── fraud-detection-service/
│
├── notification-service/
│
├── docker-compose.yml
│
└── README.md

⚙️ Configuration

Before running the project, configure:

MySQL
spring.datasource.url=jdbc:mysql://localhost:3306/<database>
spring.datasource.username=root
spring.datasource.password=<password>
Kafka

Configure the Kafka broker used by all services.

Example:

spring.kafka.bootstrap-servers=localhost:9092
Redis
spring.data.redis.host=localhost
spring.data.redis.port=6379
Razorpay

Set environment variables:

RAZORPAY_KEY_ID
RAZORPAY_KEY_SECRET
RAZORPAY_WEBHOOK_SECRET

Do not commit real Razorpay credentials to GitHub.

▶️ How to Run
1. Start MySQL

Create the required database.

CREATE DATABASE banking;
2. Start Redis

Run Redis locally:

localhost:6379
3. Start Kafka

Run Kafka and the required broker configuration.

localhost:9092

Create/configure the required topics.

4. Configure Environment Variables

Windows example:

$env:RAZORPAY_KEY_ID="your_key"
$env:RAZORPAY_KEY_SECRET="your_secret"
$env:RAZORPAY_WEBHOOK_SECRET="your_webhook_secret"
5. Start Microservices

Start services in this order:

Account Service
Transaction Service
Fraud Detection Service
Payment Service
Notification Service
API Gateway

🧪 Example Transaction
Request
POST /transactions
Content-Type: application/json
Idempotency-Key: TXN-10001

Example body:

{
  "senderAccount": "123456789012",
  "receiverAccount": "987654321098",
  "amount": 5000
}

📌 Example Event Flow
transaction.initiated
        ↓
Fraud Detection
        ↓
fraud.check.clean
        ↓
Transaction Completion
        ↓
transaction.completed
        ↓
Account Credit
        ↓
Notification

Suspicious transaction:

transaction.initiated
        ↓
Fraud Detection
        ↓
verification.required
        ↓
OTP
        ↓
Wrong OTP × 3
        ↓
Refund
        ↓
Account Block
        ↓
fraud.detected
        ↓
Notification

📚 Key Concepts Demonstrated

This project is useful for understanding:

Java
Spring Boot
Microservices
REST APIs
Spring Cloud Gateway
OpenFeign
Apache Kafka
Kafka Consumer Groups
Event-Driven Architecture
Redis
MySQL
JPA / Hibernate
Razorpay
Webhooks
Idempotency
Saga Pattern
Compensation
Transactional Outbox
Eventual Consistency
Distributed Transactions
Rate Limiting
Fraud Detection
OTP Verification

🎯 Interview Concepts Covered

This project can be discussed in backend interviews around:

Microservices
Why microservices?
Service boundaries
Database per service
Synchronous vs asynchronous communication
API Gateway
Kafka
Producer vs Consumer
Consumer groups
Partitioning
Offset
At-least-once delivery
Retry
Dead Letter Topic
Idempotent consumers
Distributed Transactions
Why @Transactional cannot cover multiple services
Saga Pattern
Compensation
Eventual consistency
Transactional Outbox
Redis
Caching
TTL
Atomic counters
Rate limiting
OTP storage
Fraud velocity detection
Database
JPA
Transactions
Row locking
Unique constraints
Idempotency
Concurrency
Payment
Razorpay order creation
Payment webhook
Signature verification
Webhook retry
Payment idempotency

🧠 What This Project Demonstrates

The primary objective of this project is not simply CRUD.

It demonstrates how to design a backend system where:

Multiple Services
       +
Multiple Databases
       +
Kafka Events
       +
Redis
       +
External Payment Gateway
       +
Failure Handling
       +
Compensation

must work together despite the absence of a single distributed database transaction.

⚠️ Disclaimer

This project is intended for educational and portfolio purposes.

It is not production-ready banking software and does not implement all security, compliance, audit, operational, and regulatory requirements expected from a real banking platform.

Do not use this implementation for processing real financial transactions without significant additional security and reliability work.

👨‍💻 Author

Vivek Chaudhari

Backend Developer | Java | Spring Boot | Microservices

⭐ If You Like This Project

If this project helped you understand:

Spring Boot Microservices
Kafka
Redis
Saga Pattern
Transactional Outbox
Fraud Detection
Distributed Transactions

consider giving the repository a ⭐ on GitHub.

📜 License

This project is available for educational and portfolio purposes.

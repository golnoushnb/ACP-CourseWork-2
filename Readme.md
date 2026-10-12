# Advanced Programming Concepts — Coursework 2

A Java-based backend application developed for the **Advanced Programming Concepts (ACP)** coursework at the University of Edinburgh. The project uses Spring Boot and brings together message brokers and an in-memory data store in a containerised development environment.

## Technology stack

| Technology | Role |
| --- | --- |
| Java 21 | Application language |
| Spring Boot 4.0.5 | Backend application framework |
| Maven | Dependency management and build |
| Apache Kafka | Event-streaming infrastructure |
| RabbitMQ | Message-broker infrastructure |
| Redis | In-memory data store |
| Docker / Docker Compose | Containerisation and local infrastructure |

The repository includes Spring Boot dependencies for Kafka, AMQP/RabbitMQ, Redis, web endpoints, and validation. The exact application behaviour and API endpoints should be checked in the source code under `src/`.

## Repository structure

```text
ACP-CourseWork-2/
├── src/                  # Application and test source code
├── .mvn/wrapper/         # Maven Wrapper configuration
├── Dockerfile            # Multi-stage application image build
├── docker-compose.yml    # Kafka, RabbitMQ, and Redis services
├── pom.xml               # Maven configuration and dependencies
├── mvnw                  # Maven Wrapper (macOS/Linux)
└── mvnw.cmd              # Maven Wrapper (Windows)
```

## Prerequisites

- Docker and Docker Compose
- Java 21 (for running outside Docker)

## Getting started

**1. Clone the project**

```bash
git clone https://github.com/golnoushnb/ACP-CourseWork-2.git
cd ACP-CourseWork-2
```

**2. Create the Docker network**

The Compose configuration uses an external network named `acp_network`:

```bash
docker network create acp_network
```

If the network already exists, skip this command.

**3. Start the infrastructure**

```bash
docker compose up -d
```

This starts Kafka, RabbitMQ, and Redis. The configuration exposes the following ports to the host:

| Service | Port |
| --- | --- |
| Kafka | `9092` |
| RabbitMQ (AMQP) | `5672` |
| RabbitMQ Management UI | `15672` |
| Redis | `6379` |
| RedisInsight | `8001` |

**4. Build and run the Spring Boot application**

```bash
./mvnw clean package
./mvnw spring-boot:run
```

On Windows, use `mvnw.cmd` instead of `./mvnw`.

Alternatively, build an application image using the provided multi-stage Dockerfile:

```bash
docker build -t acp-coursework-2 .
```

> **Configuration note:** The provided `docker-compose.yml` starts the infrastructure services but does not define a container for the Spring Boot application. Broker and Redis connection settings may need to be configured for the environment in which the application runs. The commands above are setup instructions, not a claim that the full system has been integration-tested.

## Testing

Run the project's Maven tests with:

```bash
./mvnw test
```

## Learning focus

This coursework provides hands-on experience with Java backend development, integrating messaging technologies, managing application dependencies, and setting up supporting services using Docker.

## Context

Developed as university coursework for the **Advanced Programming Concepts** course at the **University of Edinburgh**.

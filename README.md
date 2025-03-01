# Website Visit Counter

## Overview
This project is a submission for the *Website Visit Counter* assignment. It efficiently tracks page visits using caching strategies, Redis for global caching, and scalability techniques such as *sharding, batching, and consistent hashing*.

## Features
* **Basic Visit Counter**: Tracks visits to a page.
* **Redis Integration**: Stores visit counts persistently.
* **Application Layer Caching**: Reduces redundant Redis reads.
* **Batching Write Requests**: Optimizes write operations.
* **Redis Sharding**: Ensures scalability and fault tolerance.

## Tech Stack
* **Backend**: FastAPI (Python)
* **Database**: Redis
* **Containerization**: Docker

## Installation & Setup

### Prerequisites
Ensure the following are installed:
* Python 3.8+
* Docker & Docker Compose
* Redis

### Steps to Run
1. Clone this repository:
   ```bash
   git clone https://github.com/kohantikanath/Website_Visit_Counter-HLD-.git
   cd visit_counter_assignment
   ```

2. Set up environment variables (create a `.env` file in the root directory):
   ```env
   REDIS_NODES=redis://redis1:7070,redis://redis2:7071
   ```

3. Start the application and Redis instances using Docker:
   ```bash
   docker-compose up --build
   ```

4. The FastAPI server will be available at `http://localhost:8000`

## API Endpoints

### 1. Record a Page Visit
* **Endpoint:** `POST /visit/{page_id}`
* **Description:** Increments the visit count for a given page.
* **Response:**
   ```json
   { "status": "success", "message": "Visit recorded for page {page_id}" }
   ```

### 2. Get Visit Count
* **Endpoint:** `GET /visits/{page_id}`
* **Description:** Retrieves the visit count for a specific page.
* **Response Format:**
   ```json
   { "count": 123, "served_via": "redis_7070" }
   ```
   * `served_via`: Indicates if data was fetched from *in-memory cache* or a *Redis shard*.

## System Design

### 1. Application Layer Caching
* Uses an *in-memory dictionary* to temporarily store visit counts.
* Cache expiry time: *5 seconds*.
* If cache expires, fetches count from Redis.

### 2. Batching Write Requests
* Stores visit counts in an *in-memory buffer* before writing to Redis.
* Flushes the buffer to Redis *every 30 seconds* to reduce write operations.

### 3. Redis Sharding with Consistent Hashing
* Distributes keys across multiple Redis instances (*redis_7070, redis_7071*).
* Ensures scalability and fault tolerance.

## Additional Features

### 1. Monitoring and Logging
* Integrated logging using Python's `logging` module.
* Logs important events such as cache hits/misses, Redis operations, and errors.
* Monitoring endpoints to check the health of the application and Redis instances.

### 2. Rate Limiting
* Implemented rate limiting to prevent abuse of the API.
* Limits the number of requests per IP address within a specified time frame.

### 3. Graceful Shutdown
* Ensures that the application shuts down gracefully, completing any ongoing operations before exiting.
* Handles termination signals to clean up resources properly.


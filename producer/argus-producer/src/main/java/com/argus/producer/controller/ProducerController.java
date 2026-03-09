package com.argus.producer.controller;

import com.argus.producer.generator.TransactionGenerator;
import com.argus.producer.publisher.PubSubPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
public class ProducerController {

    private static final Logger log = LoggerFactory.getLogger(ProducerController.class);

    @Autowired
    private TransactionGenerator generator;

    @Autowired
    private PubSubPublisher publisher;

    // Scheduler
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    //  Store it so we can cancel it on /stop
    private ScheduledFuture<?> scheduledTask;

    // State
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Stored so /status can report what rate it is and uptime
    private volatile int currentRate = 0;
    private volatile Instant startTime;

    // Endpoints
    @GetMapping("/start")
    public Map<String, Object> start(@RequestParam(defaultValue = "5") int rate) {
        // compareAndSet(false, true) only succeeds if currently stopped
        if (!running.compareAndSet(false, true)) {
            return Map.of(
                "status", "already_running",
                "rate", currentRate,
                "tip", "Call /stop first to change the rate"
            );
        }

        // clamp the rate to safe bounds [1, 500]
        rate = Math.max(1, Math.min(500, rate));
        currentRate = rate;
        startTime = Instant.now();

        // convert rate to period
        // rate=10 -> period=100ms
        long periodMs = 1000L / rate;

        scheduledTask = scheduler.scheduleAtFixedRate(
            this::publishOnce,  //  runs on the scheduler thread
            0,                  // start immediately
            periodMs,
            TimeUnit.MILLISECONDS
        );

        log.info("Producer started: {} msg/sec (1 message every {}ms)", rate, periodMs);

        return Map.of(
            "status", "started",
            "rate", rate,
            "period_ms", periodMs
        );
    }

    // Stop publishing transactions
    @GetMapping("/stop")
    public Map<String, Object> stop() {
        if (!running.compareAndSet(true, false)) {
            return Map.of("status", "already_stopped");
        }

        // cancel the scheduled task
        // false = don't interrupt if publish is currently mid execution
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }

        long totalPublished = publisher.getPublishedCount();
        log.info("Producer stopped. Total published: {}", totalPublished);

        return Map.of(
            "status", "stopped",
            "total_published", totalPublished
        );
    }

    // Get current producer state
    @GetMapping("/status")
    public Map<String, Object> status() {
        long uptimeSeconds = 0;
        if (startTime != null && running.get()) {
            uptimeSeconds = Instant.now().getEpochSecond() - startTime.getEpochSecond();
        }

        return Map.of(
            "running",         running.get(),
            "rate",            currentRate,
            "published",       publisher.getPublishedCount(),
            "failed",          publisher.getFailedCount(),
            "uptime_seconds",  uptimeSeconds
        );
    }

    private void publishOnce() {
        try {
            publisher.publish(generator.generate());
        } catch (Exception e) {
            log.error("publishOnce failed: {}", e.getMessage());
        }
    }


}

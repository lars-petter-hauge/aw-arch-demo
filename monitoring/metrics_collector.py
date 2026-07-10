"""Metrics Collector for AW Arch Demo

This module handles collection of metrics from K8s and RabbitMQ.
Separated from main application logic for clean architecture.
"""

import asyncio
import json
import logging
from typing import Dict, List, Any
from datetime import datetime

import aio_pika

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects metrics from K8s pods and RabbitMQ."""

    def __init__(self, broker_url: str):
        """Initialize metrics collector.

        Args:
            broker_url: RabbitMQ connection URL
        """
        self.broker_url = broker_url
        self.connection = None
        self.channel = None

    async def connect(self):
        """Connect to RabbitMQ."""
        try:
            self.connection = await aio_pika.connect_robust(self.broker_url)
            self.channel = await self.connection.channel()
            logger.info("Connected to RabbitMQ for metrics collection")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def close(self):
        """Close RabbitMQ connection."""
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()
        logger.info("Closed RabbitMQ connection")

    async def get_queue_depth(self, queue_name: str) -> int:
        """Get message count in a queue.

        Args:
            queue_name: Name of the queue

        Returns:
            Number of messages in queue
        """
        try:
            if not self.channel:
                await self.connect()

            queue = await self.channel.get_queue(queue_name, ensure=False)
            if queue:
                message_count = queue.declaration_result.method.message_count
                return message_count
            return 0
        except Exception as e:
            logger.warning(f"Could not get queue depth for {queue_name}: {e}")
            return 0

    async def get_queue_depths(self) -> Dict[str, int]:
        """Get message counts for all worker queues.

        Returns:
            Dictionary of queue names to message counts
        """
        depths = {}
        for worker in ["worker_a", "worker_b", "worker_c"]:
            queue_name = f"jobs.{worker}"
            depth = await self.get_queue_depth(queue_name)
            depths[queue_name] = depth
        return depths

    async def get_metrics_json(self) -> Dict[str, Any]:
        """Get all metrics as JSON.

        Returns:
            Dictionary with all metrics
        """
        try:
            queue_depths = await self.get_queue_depths()
            total_jobs = sum(queue_depths.values())

            return {
                "timestamp": datetime.utcnow().isoformat(),
                "queues": queue_depths,
                "total_jobs_waiting": total_jobs,
            }
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            }

    def get_prometheus_metrics(self) -> str:
        """Get metrics in Prometheus format.

        Returns:
            Prometheus-formatted metrics string
        """
        # Placeholder for async loop if needed
        # In production, collect async metrics and format them
        return "# TYPE aw_arch_queue_depth gauge\n"

"""
GitHub Events Ingestion Service

Polls GitHub Events API and publishes to Kafka with:
- Rate limit handling
- Deduplication (LRU cache)
- Exponential backoff retries with jitter
- Error resilience
- Comprehensive logging & metrics

"""

import os
import sys
import json
import time
import logging
import random
from collections import OrderedDict
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration loaded from environment variables."""

    # GitHub API
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_API_URL = os.getenv("GITHUB_API_URL", "https://api.github.com/events")
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))  # seconds

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github.events")

    # Deduplication
    CACHE_SIZE = int(os.getenv("CACHE_SIZE", "100000"))

    # Retry
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    BASE_RETRY_DELAY = int(os.getenv("BASE_RETRY_DELAY", "2"))

    @classmethod
    def validate(cls) -> None:
        if not cls.GITHUB_TOKEN or cls.GITHUB_TOKEN == "your_github_token_here":
            raise ValueError(
                "GITHUB_TOKEN is missing or invalid. "
                "Create one at https://github.com/settings/tokens"
            )


# ============================================================================
# DATA MODEL
# ============================================================================

class GitHubEvent:
    """
    Lightweight GitHub event wrapper.

    Performs minimal validation and normalization.
    Heavy validation is intentionally deferred downstream.
    """

    REQUIRED_FIELDS = ["id", "type", "actor", "repo", "created_at"]

    def __init__(self, raw: Dict[str, Any]):
        missing = [f for f in self.REQUIRED_FIELDS if f not in raw]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        self.id = str(raw["id"])
        self.type = raw["type"]
        self.actor = raw["actor"]
        self.repo = raw["repo"]
        self.created_at = raw["created_at"]
        self.payload = raw.get("payload", {})
        self.public = raw.get("public", True)
        self.org = raw.get("org")

    def to_dict(self) -> Dict[str, Any]:
        """Return JSON-serializable representation."""
        return {
            "id": self.id,
            "type": self.type,
            "actor": {
                "id": self.actor.get("id"),
                "login": self.actor.get("login"),
                "url": self.actor.get("url"),
            },
            "repo": {
                "id": self.repo.get("id"),
                "name": self.repo.get("name"),
                "url": self.repo.get("url"),
            },
            "created_at": self.created_at,
            "payload": self.payload,
            "public": self.public,
            "org": {"id": self.org.get("id")} if self.org else None,
        }


# ============================================================================
# DEDUPLICATION CACHE
# ============================================================================

class LRUCache:
    """Bounded LRU cache for event ID deduplication."""

    def __init__(self, max_size: int):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def contains(self, key: str) -> bool:
        if key in self.cache:
            self.cache.move_to_end(key)
            self.hits += 1
            return True
        self.misses += 1
        return False

    def add(self, key: str) -> None:
        self.cache[key] = True
        self.cache.move_to_end(key)
        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0


# ============================================================================
# EXCEPTIONS
# ============================================================================

class RetryableError(Exception):
    """Signals that an operation can be retried safely."""
    pass


# ============================================================================
# PRODUCER
# ============================================================================

class GitHubProducer:
    """
    Polls GitHub Events API and publishes events to Kafka.

    Guarantees:
    - At-least-once delivery to Kafka
    - Idempotent ingestion via deduplication
    """

    def __init__(self):
        self.config = Config
        self.session = self._create_http_session()
        self.producer = self._create_kafka_producer()
        self.cache = LRUCache(self.config.CACHE_SIZE)

        self.metrics = {
            "polls": 0,
            "fetched": 0,
            "published": 0,
            "duplicates": 0,
            "invalid": 0,
            "errors": 0,
        }

        logger.info("GitHub Producer initialized")

    # ---------------------------------------------------------------------

    def _create_http_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"token {self.config.GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "realtime-github-analytics",
            }
        )
        return session

    def _create_kafka_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type="gzip",
        )

    # ---------------------------------------------------------------------

    def fetch_events(self) -> List[Dict[str, Any]]:
        try:
            resp = self.session.get(self.config.GITHUB_API_URL, timeout=10)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 403:
                reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                sleep_for = max(reset - time.time(), 60)
                logger.warning(f"Rate limited. Sleeping {sleep_for:.0f}s")
                time.sleep(sleep_for)
                return []

            if resp.status_code in {500, 502, 503, 504}:
                raise RetryableError(f"GitHub server error {resp.status_code}")

            if resp.status_code == 401:
                logger.error("Invalid GitHub token")
                sys.exit(1)

            logger.error(f"Unexpected status {resp.status_code}")
            return []

        except (requests.Timeout, requests.ConnectionError) as e:
            raise RetryableError(str(e))

    # ---------------------------------------------------------------------

    def publish_event(self, event: GitHubEvent) -> None:
        try:
            future = self.producer.send(
                self.config.KAFKA_TOPIC,
                key=event.id,          # IMPORTANT: deterministic partitioning
                value=event.to_dict(),
            )
            future.get(timeout=10)
        except KafkaTimeoutError:
            raise RetryableError("Kafka timeout")
        except KafkaError as e:
            if "broker" in str(e).lower():
                raise RetryableError(str(e))
            raise

    # ---------------------------------------------------------------------

    def retry(self, fn, *args):
        for attempt in range(self.config.MAX_RETRIES):
            try:
                return fn(*args)
            except RetryableError as e:
                if attempt == self.config.MAX_RETRIES - 1:
                    raise
                delay = min(
                    self.config.BASE_RETRY_DELAY * (2 ** attempt), 60
                )
                delay += random.uniform(0, delay * 0.1)
                logger.warning(f"Retrying in {delay:.1f}s: {e}")
                time.sleep(delay)

    # ---------------------------------------------------------------------

    def process_poll(self) -> None:
        self.metrics["polls"] += 1

        try:
            events = self.retry(self.fetch_events)
            self.metrics["fetched"] += len(events)

            for raw in events:
                try:
                    event = GitHubEvent(raw)
                except ValueError:
                    self.metrics["invalid"] += 1
                    continue

                if self.cache.contains(event.id):
                    self.metrics["duplicates"] += 1
                    continue

                self.retry(self.publish_event, event)

                # Only mark as seen AFTER successful publish
                self.cache.add(event.id)
                self.metrics["published"] += 1

        except Exception as e:
            self.metrics["errors"] += 1
            logger.exception(f"Poll failed: {e}")

        logger.info(
            f"Poll {self.metrics['polls']} | "
            f"published={self.metrics['published']} "
            f"duplicates={self.metrics['duplicates']} "
            f"cache_hit_rate={self.cache.hit_rate:.1%}"
        )

    # ---------------------------------------------------------------------

    def run(self) -> None:
        logger.info("Starting GitHub ingestion loop")
        try:
            while True:
                self.process_poll()
                time.sleep(self.config.POLL_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Shutting down producer")
            self.producer.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    Config.validate()
    GitHubProducer().run()


if __name__ == "__main__":
    main()

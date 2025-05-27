import time
import requests
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, retry_if_exception
from pybreaker import CircuitBreaker, CircuitBreakerListener
from infrastructure.observability import metrics
from typing import Any, Dict, Optional

from infrastructure.observability.metrics import (
    api_request_duration_hist,
    api_errors_counter,
    pipedrive_api_call_total,
    pipedrive_api_rate_limit_remaining,
    pipedrive_api_rate_limit_reset_seconds,
    pipedrive_api_retry_failures_total
)

log = structlog.get_logger(__name__)

class BreakerMetricsListener(CircuitBreakerListener):
    def state_changed(self, cb, old_state, new_state):
        if new_state.name == "open":
            log.warn(f"Circuit breaker '{cb.name}' opened.") 
            metrics.pipedrive_api_breaker_open_total.inc() 
        elif new_state.name == "closed" and old_state.name == "open":
            log.info(f"Circuit breaker '{cb.name}' closed.")
            
class BasePipedriveAPIClient:
    BASE_URL_V1 = "https://api.pipedrive.com/v1"
    BASE_URL_V2 = "https://api.pipedrive.com/api/v2"
    DEFAULT_TIMEOUT = 45

    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.api_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
        self.log = log.bind(client="BasePipedriveAPIClient")
        self.api_breaker = CircuitBreaker(fail_max=3, reset_timeout=60, name="PipedriveAPIBreaker")
        self.api_breaker.add_listener(BreakerMetricsListener())
        
    def _normalize_endpoint(self, url: str) -> str:
        if url.startswith(self.BASE_URL_V1):
            path = url[len(self.BASE_URL_V1):]
        elif url.startswith(self.BASE_URL_V2):
            path = url[len(self.BASE_URL_V2):]
        else:
            path = url
        path = path.split("?")[0].strip("/")
        return "/" + path

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=(retry_if_exception_type(requests.exceptions.Timeout) |
               retry_if_exception_type(requests.exceptions.ConnectionError) |
               retry_if_exception(lambda e: isinstance(e, requests.exceptions.HTTPError) and e.response and e.response.status_code >= 500) |
               retry_if_exception(lambda e: isinstance(e, requests.exceptions.HTTPError) and e.response and e.response.status_code == 429)),
        reraise=True
    )
    @property
    def _breaker(self):
        return self.api_breaker

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        params = params or {}
        if "api_token" not in params:
            params["api_token"] = self.api_key

        start_time = time.monotonic()
        response = None
        error_type = "success"
        status_code = None

        normalized_endpoint = self._normalize_endpoint(url)
        request_log = self.log.bind(endpoint=normalized_endpoint, method="GET")

        try:
            request_log.debug("Making API GET request", url=url)
            response = self.session.get(url, params=params, timeout=self.DEFAULT_TIMEOUT)
            status_code = response.status_code
            pipedrive_api_call_total.labels(endpoint=normalized_endpoint, method='GET', status_code=str(status_code)).inc()

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                request_log.warning("Rate limit hit (429)", retry_after=retry_after)

            response.raise_for_status()

            remaining = response.headers.get('X-RateLimit-Remaining')
            if remaining and remaining.isdigit():
                pipedrive_api_rate_limit_remaining.labels(endpoint=normalized_endpoint).set(int(remaining))

            reset = response.headers.get('X-RateLimit-Reset')
            if reset and reset.isdigit():
                pipedrive_api_rate_limit_reset_seconds.labels(endpoint=normalized_endpoint).set(int(reset))

            duration = time.monotonic() - start_time
            api_request_duration_hist.labels(endpoint=normalized_endpoint, method="GET", status_code=str(status_code)).observe(duration)

            request_log.debug("API GET successful", duration_sec=f"{duration:.3f}s")
            return response

        except Exception as e:
            duration = time.monotonic() - start_time
            if isinstance(e, requests.exceptions.Timeout):
                error_type = "timeout"
            elif isinstance(e, requests.exceptions.ConnectionError):
                error_type = "connection_error"
            elif isinstance(e, requests.exceptions.RequestException):
                error_type = "request_exception"
            api_errors_counter.labels(endpoint=normalized_endpoint, error_type=error_type, status_code=str(status_code or 'N/A')).inc()
            pipedrive_api_retry_failures_total.labels(endpoint=normalized_endpoint).inc()
            request_log.error("API GET failed", error=str(e), duration_sec=f"{duration:.3f}s")
            raise

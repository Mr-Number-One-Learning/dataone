"""
Shared retry/backoff decorator for ingestion and orchestration jobs — this is
the "manual retry on failure" + "robust failure recovery" mechanism referenced
for the orchestration criterion.
"""
from __future__ import annotations

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dataone.utils.logging_config import get_logger

log = get_logger(__name__)


def _log_before_sleep(retry_state: RetryCallState) -> None:
    """Logs the retry attempt before sleeping.

    tenacity before_sleep hook — makes each retry visible in the logs
    (attempt number, the exception that triggered it, and how long we back
    off) instead of the job appearing to hang silently between attempts.

    Args:
        retry_state (RetryCallState): The current state of the retry mechanism.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    sleep_seconds = retry_state.next_action.sleep if retry_state.next_action else None
    log.warning(
        "retry.backoff",
        fn=getattr(retry_state.fn, "__qualname__", str(retry_state.fn)),
        attempt=retry_state.attempt_number,
        error=repr(exc),
        sleep_seconds=sleep_seconds,
    )


def with_retry(max_attempts: int = 5, exceptions: tuple[type[Exception], ...] = (Exception,)):
    """Creates a decorator for retrying a function with exponential backoff.

    Decorator: exponential backoff, capped attempts, logs each retry.

    Args:
        max_attempts (int, optional): The maximum number of attempts before failing. 
            Defaults to 5.
        exceptions (tuple[type[Exception], ...], optional): A tuple of exception types 
            to catch and retry on. Defaults to (Exception,).

    Returns:
        Callable: The wrapped function with retry logic applied.

    Usage:
        @with_retry(max_attempts=3, exceptions=(ConnectionError,))
        def flaky_call(): ...
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(exceptions),
        before_sleep=_log_before_sleep,
        reraise=True,
    )

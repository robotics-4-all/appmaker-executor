"""
Resilient MQTT transport for commlib.

commlib's ``MQTTTransport.on_disconnect`` ends with an unconditional
``raise ConnectionError()``. That callback is invoked by paho from inside its
network thread, so any broker-side disconnect tears the thread down with a
traceback and the transport is dead for the rest of the process lifetime. The
raise is only meaningful on the synchronous ``connect()`` path, which
``AppMaker.create_mqtt_interface`` already catches and retries.

This module replaces the transport callbacks at runtime (never by editing
site-packages, which ``pip install`` would overwrite) so that:

* ``on_disconnect`` never raises, and stops the paho loop cleanly.
* A supervisor thread transparently rebuilds the connection with exponential
  backoff.
* Subscriptions are recorded on the transport and replayed on every reconnect,
  because ``subscribe()`` binds its message callback to the paho client object
  and paho is rebuilt from scratch on each connect.

``install()`` is idempotent and safe to call from both the parent process and
the forked executor process.
"""

import logging
import threading
import time
from typing import List, Optional, Tuple

from commlib.transports import mqtt as commlib_mqtt

_LOGGER = logging.getLogger(__name__)

# Seconds between supervisor polls when the connection is healthy.
POLL_INTERVAL = 0.5
# Reconnect backoff bounds, in seconds.
INITIAL_RECONNECT_DELAY = 0.5
MAX_RECONNECT_DELAY = 10.0
# How long to wait for the broker's CONNACK before declaring a reconnect failed.
CONNACK_TIMEOUT = 5.0

_installed = False

# Bound originals once, at import time, so repeated install() calls cannot
# capture our own wrappers and build a recursion chain.
_orig_connect = commlib_mqtt.MQTTTransport.connect
_orig_subscribe = commlib_mqtt.MQTTTransport.subscribe
_orig_publish = commlib_mqtt.MQTTTransport.publish
_orig_stop = commlib_mqtt.MQTTTransport.stop


class _GuardState:
    """Per-transport bookkeeping used to drive reconnection."""

    def __init__(self):
        self.lock = threading.Lock()
        # (topic, callback, qos) triples replayed after every reconnect.
        self.subscriptions: List[Tuple[str, object, object]] = []
        self.stopped = False
        self.event = threading.Event()
        self.supervisor: Optional[threading.Thread] = None


def _state(transport):
    """Return the guard state for a transport, creating it on first use."""
    state = getattr(transport, "_appmaker_guard", None)
    if state is None:
        state = _GuardState()
        # Bypass any __slots__/property restrictions commlib may add later.
        setattr(transport, "_appmaker_guard", state)
    return state


def _ensure_supervisor(transport):
    state = _state(transport)

    with state.lock:
        if state.supervisor is not None and state.supervisor.is_alive():
            return
        thread = threading.Thread(
            target=_supervise,
            args=(transport,),
            daemon=True,
            name="appmaker-mqtt-supervisor",
        )
        state.supervisor = thread
        # Started under the lock: a caller that saw the unstarted thread as
        # not-alive would otherwise spawn a second supervisor for the same
        # transport, orphaning a paho client and double-delivering messages.
        thread.start()


def _replay_subscriptions(transport, state):
    """Re-register subscriptions against the freshly built paho client."""
    with state.lock:
        pending = list(state.subscriptions)

    for topic, callback, qos in pending:
        try:
            _orig_subscribe(transport, topic, callback, qos)
        except Exception as exc:  # pylint: disable=broad-except
            _LOGGER.warning("Could not restore subscription to %s: %s", topic, exc)


def _wait_for_connack(transport, timeout=CONNACK_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if transport.is_connected:
            return True
        time.sleep(0.05)
    return transport.is_connected


def _try_reconnect(transport, state):
    try:
        _orig_connect(transport)
        # commlib only calls transport.start() from the endpoint's run(), which
        # happens once. The rebuilt paho client therefore needs its network
        # loop started explicitly or it stays connected but deaf.
        transport.start()
    except Exception as exc:  # pylint: disable=broad-except
        _LOGGER.warning("MQTT reconnect attempt failed: %s", exc)
        return False

    if not _wait_for_connack(transport):
        _LOGGER.warning("MQTT reconnect timed out waiting for CONNACK")
        return False

    _replay_subscriptions(transport, state)
    _LOGGER.info("MQTT connection restored")
    return True


def _supervise(transport):
    state = _state(transport)
    delay = INITIAL_RECONNECT_DELAY

    while not state.stopped:
        state.event.wait(POLL_INTERVAL)
        state.event.clear()

        if state.stopped:
            break

        try:
            if transport.is_connected:
                delay = INITIAL_RECONNECT_DELAY
                continue

            if _try_reconnect(transport, state):
                delay = INITIAL_RECONNECT_DELAY
                continue
        except Exception as exc:  # pylint: disable=broad-except
            _LOGGER.error("MQTT supervisor error: %s", exc)

        state.event.wait(delay)
        state.event.clear()
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


def _on_disconnect(self, client, userdata, rc, unk=None):  # type: ignore[override] # pylint: disable=unused-argument
    self._connected = False

    if rc == 5:
        _LOGGER.error("MQTT authentication error with broker")
    elif rc != 0:
        _LOGGER.warning("MQTT disconnected unexpectedly (rc=%s)", rc)

    try:
        client.loop_stop()
    except Exception:  # pylint: disable=broad-except
        pass

    state = _state(self)
    _ensure_supervisor(self)
    # Nudge the supervisor so recovery starts now rather than at the next poll.
    state.event.set()


def _subscribe(self, topic, callback, qos=commlib_mqtt.MQTTQoS.L0):
    """Record the subscription so it survives a paho client rebuild."""
    state = _state(self)
    with state.lock:
        state.subscriptions.append((topic, callback, qos))
    _ensure_supervisor(self)
    return _orig_subscribe(self, topic, callback, qos)


def _publish(self, topic, payload, qos=commlib_mqtt.MQTTQoS.L0, retain=False):
    """Publish, tolerating a transport that is currently down."""
    if self._client is None:
        _LOGGER.warning("Dropping publish to %s: MQTT client not initialised", topic)
        return

    if not self.is_connected:
        _LOGGER.warning("Dropping publish to %s: MQTT transport is down", topic)
        return

    return _orig_publish(self, topic, payload, qos=qos, retain=retain)


def _stop(self):
    """Stop the transport and retire its supervisor thread."""
    state = _state(self)
    state.stopped = True
    state.event.set()
    return _orig_stop(self)


def install():
    """Install the guarded MQTT callbacks. Idempotent."""
    global _installed
    if _installed:
        return

    commlib_mqtt.MQTTTransport.on_disconnect = _on_disconnect  # type: ignore[assignment]
    commlib_mqtt.MQTTTransport.subscribe = _subscribe  # type: ignore[assignment]
    commlib_mqtt.MQTTTransport.publish = _publish  # type: ignore[assignment]
    commlib_mqtt.MQTTTransport.stop = _stop  # type: ignore[assignment]

    _installed = True
    _LOGGER.info("MQTT guard installed")
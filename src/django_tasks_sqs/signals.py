"""Signals sent by the worker, on top of the ``django.tasks`` ones."""

from django.dispatch import Signal

#: Sent after the worker handles a received message, whatever the outcome.
#: Arguments: ``worker``, ``queue_name``, ``outcome`` (an ``Outcome`` value) and
#: ``duration`` (seconds). Handy for pushing metrics to CloudWatch, StatsD…
message_processed = Signal()

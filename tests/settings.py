SECRET_KEY = "test"
USE_TZ = True
INSTALLED_APPS = ["django_tasks_sqs"]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
TASKS = {
    "default": {
        "BACKEND": "django_tasks_sqs.SQSBackend",
        "QUEUES": ["default", "emails", "orders.fifo"],
        "OPTIONS": {"queue_name_prefix": "test-", "region_name": "eu-west-1"},
    },
    "immediate": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"},
}

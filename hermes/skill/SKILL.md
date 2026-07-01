# Caduceus Remote Prompt Delegation

Use Caduceus only for trusted LAN worker delegation.

Before task execution:

1. Call `caduceus.list_workers`.
2. Choose an explicitly trusted worker or use `auto` only when the user accepts local trust assumptions.
3. Call `caduceus.validate_task` before `caduceus.run_remote_task`.

During execution:

1. Use `caduceus.get_task_events` to show streamed progress.
2. Use `caduceus.get_task_result` for final output.
3. Use `caduceus.cancel_task` when the user asks to stop work.

Safety:

Never send secrets, credentials, private source code, or sensitive personal data to a worker unless the user explicitly asks and the target worker is trusted.

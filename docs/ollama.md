# Ollama

The default backend is Ollama through its OpenAI-compatible API:

```text
http://127.0.0.1:11434/v1
```

Caduceus uses `POST /chat/completions` for tasks and `GET /models` for model discovery when available. It does not require Ollama-specific task APIs.

Check:

```bash
caduceusctl ollama check
```

Install:

```bash
caduceusctl ollama install
```

Install commands prompt before taking action.

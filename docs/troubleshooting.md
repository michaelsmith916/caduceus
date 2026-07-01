# Troubleshooting

## Daemon Not Reachable

Run:

```bash
caduceusctl status
```

If it cannot connect, start `caduceusd` and check the configured control endpoint with `caduceusctl config show`.

## No Workers

Confirm both nodes have the same group key hash and that each peer ID is in the other node's allowlist.

## Ollama Errors

Run:

```bash
caduceusctl ollama check
```

Confirm the OpenAI-compatible endpoint responds at `/v1/models`.

## WSL mDNS

Some WSL networking modes limit multicast. If mDNS discovery fails, use local tasks or test on a network mode that supports multicast.

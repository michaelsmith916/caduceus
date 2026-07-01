# Configuration

Default config paths:

- Linux/WSL: `~/.config/caduceus/config.yaml`
- macOS: `~/Library/Application Support/Caduceus/config.yaml`
- Windows: `%APPDATA%\Caduceus\config.yaml`

Default data paths:

- Linux/WSL: `~/.local/share/caduceus`
- macOS: `~/Library/Application Support/Caduceus`
- Windows: `%LOCALAPPDATA%\Caduceus`

Run:

```bash
caduceusctl init
caduceusctl config show
caduceusctl key generate
```

Peers are managed separately:

```bash
caduceusctl peers add 12D3Koo... --name office-rtx5090 --trust-level trusted-lan
caduceusctl peers list
```

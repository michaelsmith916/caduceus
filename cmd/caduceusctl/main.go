package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/app"
	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/control"
	cryptoutil "github.com/caduceus/caduceus/internal/crypto"
	"github.com/caduceus/caduceus/internal/localnode"
	"github.com/caduceus/caduceus/internal/ollama"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/pkg/caduceus"
)

type cliOptions struct {
	configPath string
	json       bool
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "caduceusctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	opts, args, err := parseGlobal(args)
	if err != nil {
		return err
	}
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		usage()
		return nil
	}
	ctx := context.Background()
	switch args[0] {
	case "status":
		return withClient(ctx, opts, func(c *control.Client) error {
			resp, err := c.Status(ctx)
			return printResponse(resp, err, opts.json)
		})
	case "config":
		return configCommand(opts, args[1:])
	case "init":
		return initCommand(opts)
	case "key":
		return keyCommand(opts, args[1:])
	case "peers":
		return peersCommand(opts, args[1:])
	case "workers":
		return workersCommand(ctx, opts, args[1:])
	case "tasks":
		return tasksCommand(ctx, opts, args[1:])
	case "run-prompt":
		return runPromptCommand(ctx, opts, args[1:])
	case "route":
		return routeCommand(ctx, opts, args[1:])
	case "enrollment":
		return enrollmentCommand(ctx, opts, args[1:])
	case "ollama":
		return ollamaCommand(ctx, opts, args[1:])
	case "version", "--version":
		fmt.Println(caduceus.Version)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func parseGlobal(args []string) (cliOptions, []string, error) {
	var opts cliOptions
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--json":
			opts.json = true
		case "--config":
			if i+1 >= len(args) {
				return opts, nil, errors.New("--config requires a path")
			}
			opts.configPath = args[i+1]
			i++
		default:
			out = append(out, args[i])
		}
	}
	return opts, out, nil
}

func usage() {
	fmt.Printf(`caduceusctl %s

Usage:
  caduceusctl [--config path] [--json] status
  caduceusctl [--config path] [--json] config show|path
  caduceusctl [--config path] init
  caduceusctl [--config path] key generate
  caduceusctl [--config path] peers list|add|remove
  caduceusctl [--config path] workers list|get
  caduceusctl [--config path] tasks list|get|events|cancel
  caduceusctl [--config path] run-prompt --worker auto --prompt "..."
  caduceusctl [--config path] route --explain [--output human|json] task.json|-
  caduceusctl [--config path] enrollment invite|list|get|approve|deny|audit
  caduceusctl [--config path] enrollment request --coordinator <multiaddr> --token-file <path|->
  caduceusctl [--config path] enrollment status <request_id> [--coordinator <multiaddr>]
  caduceusctl ollama check|install

`, caduceus.Version)
}

func configCommand(opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("config command requires show or path")
	}
	cfg, path, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	switch args[0] {
	case "path":
		if opts.json {
			return printJSON(map[string]any{"path": path})
		}
		fmt.Println(path)
		return nil
	case "show":
		if opts.json {
			return printJSON(cfg)
		}
		return printYAMLish(cfg)
	default:
		return fmt.Errorf("unknown config command %q", args[0])
	}
}

func initCommand(opts cliOptions) error {
	cfg, path, err := config.EnsureConfig(opts.configPath)
	if err != nil {
		return err
	}
	key, created, err := cryptoutil.LoadOrCreateSharedKey(cfg.Security.P2PKeyPath)
	if err != nil {
		return err
	}
	hash, err := cryptoutil.HashSharedKey(key)
	if err != nil {
		return err
	}
	cfg.Security.P2PKeyHash = hash
	priv, peerID, _, err := localnode.LoadOrCreateIdentity(cfg.Node.PrivateKeyPath)
	_ = priv
	if err != nil {
		return err
	}
	cfg.Node.PeerID = peerID.String()
	if err := config.Save(path, cfg); err != nil {
		return err
	}
	if _, err := os.Stat(cfg.Security.AllowedPeersPath); errors.Is(err, os.ErrNotExist) {
		if err := security.SaveAllowedPeers(cfg.Security.AllowedPeersPath, security.AllowedPeers{Peers: []security.AllowedPeer{}}); err != nil {
			return err
		}
	}
	data := map[string]any{
		"config_path":        path,
		"data_dir":           cfg.Storage.DataDir,
		"peer_id":            cfg.Node.PeerID,
		"p2p_key_created":    created,
		"p2p_key_hash":       hash,
		"allowed_peers_path": cfg.Security.AllowedPeersPath,
	}
	if opts.json {
		return printJSON(data)
	}
	fmt.Println("Initialized Caduceus")
	fmt.Println("  config:", path)
	fmt.Println("  data:", cfg.Storage.DataDir)
	fmt.Println("  peer:", cfg.Node.PeerID)
	fmt.Println("  group hash:", hash)
	return nil
}

func keyCommand(opts cliOptions, args []string) error {
	if len(args) == 0 || args[0] != "generate" {
		return errors.New("key command requires generate")
	}
	cfg, path, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	key, err := cryptoutil.GenerateSharedKey()
	if err != nil {
		return err
	}
	if err := cryptoutil.SaveSharedKey(cfg.Security.P2PKeyPath, key); err != nil {
		return err
	}
	hash, err := cryptoutil.HashSharedKey(key)
	if err != nil {
		return err
	}
	cfg.Security.P2PKeyHash = hash
	if err := config.Save(path, cfg); err != nil {
		return err
	}
	if opts.json {
		return printJSON(map[string]any{"p2p_key_path": cfg.Security.P2PKeyPath, "p2p_key_hash": hash})
	}
	fmt.Println("Generated shared P2P group key")
	fmt.Println("  key path:", cfg.Security.P2PKeyPath)
	fmt.Println("  group hash:", hash)
	return nil
}

func peersCommand(opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("peers command requires list, add, or remove")
	}
	cfg, _, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		peers, err := security.LoadAllowedPeers(cfg.Security.AllowedPeersPath)
		if err != nil {
			return err
		}
		if opts.json {
			return printJSON(peers)
		}
		if len(peers.Peers) == 0 {
			fmt.Println("No allowed peers configured.")
			return nil
		}
		for _, p := range peers.Peers {
			fmt.Printf("%s\t%s\t%s\tallowed=%v\n", p.PeerID, p.Name, p.TrustLevel, p.Allowed)
		}
		return nil
	case "add":
		if len(args) < 2 {
			return errors.New("peers add requires <peer_id>")
		}
		fs := flag.NewFlagSet("peers add", flag.ContinueOnError)
		name := fs.String("name", "", "peer name")
		trust := fs.String("trust-level", "trusted-lan", "trust level")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		peer := security.AllowedPeer{PeerID: args[1], Name: *name, TrustLevel: *trust, Allowed: true}
		if err := security.AddPeer(cfg.Security.AllowedPeersPath, peer); err != nil {
			return err
		}
		if opts.json {
			return printJSON(peer)
		}
		fmt.Println("Added peer:", args[1])
		return nil
	case "remove":
		if len(args) < 2 {
			return errors.New("peers remove requires <peer_id>")
		}
		if err := security.RemovePeer(cfg.Security.AllowedPeersPath, args[1]); err != nil {
			return err
		}
		if opts.json {
			return printJSON(map[string]any{"removed": args[1]})
		}
		fmt.Println("Removed peer:", args[1])
		return nil
	default:
		return fmt.Errorf("unknown peers command %q", args[0])
	}
}

func workersCommand(ctx context.Context, opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("workers command requires list or get")
	}
	return withClient(ctx, opts, func(c *control.Client) error {
		switch args[0] {
		case "list":
			resp, err := c.ListWorkers(ctx)
			return printResponse(resp, err, opts.json)
		case "get":
			if len(args) < 2 {
				return errors.New("workers get requires <worker_id>")
			}
			resp, err := c.GetWorker(ctx, args[1])
			return printResponse(resp, err, opts.json)
		default:
			return fmt.Errorf("unknown workers command %q", args[0])
		}
	})
}

func tasksCommand(ctx context.Context, opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("tasks command requires list, get, events, or cancel")
	}
	return withClient(ctx, opts, func(c *control.Client) error {
		switch args[0] {
		case "list":
			resp, err := c.ListTasks(ctx, "")
			return printResponse(resp, err, opts.json)
		case "get":
			if len(args) < 2 {
				return errors.New("tasks get requires <task_id>")
			}
			resp, err := c.GetTaskStatus(ctx, args[1])
			return printResponse(resp, err, opts.json)
		case "events":
			if len(args) < 2 {
				return errors.New("tasks events requires <task_id>")
			}
			follow := contains(args[2:], "--follow")
			return taskEvents(ctx, c, args[1], follow, opts.json)
		case "cancel":
			if len(args) < 2 {
				return errors.New("tasks cancel requires <task_id>")
			}
			resp, err := c.CancelTask(ctx, args[1])
			return printResponse(resp, err, opts.json)
		default:
			return fmt.Errorf("unknown tasks command %q", args[0])
		}
	})
}

func taskEvents(ctx context.Context, c *control.Client, taskID string, follow bool, jsonOut bool) error {
	var cursor int64
	for {
		resp, err := c.GetTaskEvents(ctx, taskID, cursor)
		if err != nil {
			return err
		}
		if !resp.OK {
			return printResponse(resp, nil, jsonOut)
		}
		if jsonOut && !follow {
			return printJSON(resp)
		}
		events := extractEvents(resp.Data)
		for _, event := range events {
			if event.Cursor > cursor {
				cursor = event.Cursor
			}
			fmt.Printf("%d\t%s\t%s%s\n", event.Cursor, event.EventType, event.Message, event.Delta)
		}
		if !follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func runPromptCommand(ctx context.Context, opts cliOptions, args []string) error {
	fs := flag.NewFlagSet("run-prompt", flag.ContinueOnError)
	worker := fs.String("worker", "auto", "worker peer ID or auto")
	model := fs.String("model", "", "model")
	prompt := fs.String("prompt", "", "prompt")
	system := fs.String("system", "", "system prompt")
	temp := fs.String("temperature", "", "temperature")
	maxTokens := fs.Int("max-tokens", 0, "max tokens")
	timeout := fs.Int("timeout", 0, "timeout seconds")
	idempotent := fs.Bool("idempotent", false, "allow safe redispatch after worker loss")
	maxAttempts := fs.Int("max-attempts", 0, "maximum attempts for idempotent work")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*prompt) == "" {
		return errors.New("--prompt is required")
	}
	var temperature *float64
	if *temp != "" {
		v, err := strconv.ParseFloat(*temp, 64)
		if err != nil {
			return err
		}
		temperature = &v
	}
	req := control.RunTaskRequest{
		WorkerID: *worker,
		Task: tasks.PromptTask{
			Prompt:      *prompt,
			System:      *system,
			Model:       *model,
			Temperature: temperature,
			MaxTokens:   *maxTokens,
			Stream:      true,
		},
		Constraints:    tasks.Constraints{RequiredCapabilities: []string{"llm"}},
		TimeoutSeconds: *timeout,
		TrustLevel:     "trusted-lan",
		Idempotent:     *idempotent,
		MaxAttempts:    *maxAttempts,
	}
	return withClient(ctx, opts, func(c *control.Client) error {
		resp, err := c.RunTask(ctx, req)
		return printResponse(resp, err, opts.json)
	})
}

func ollamaCommand(ctx context.Context, opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("ollama command requires check or install")
	}
	cfg, _, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	status := ollama.Check(ctx, cfg.OpenAI.BaseURL)
	switch args[0] {
	case "check":
		if opts.json {
			return printJSON(status)
		}
		fmt.Println(status.Message)
		if status.BinaryPath != "" {
			fmt.Println("binary:", status.BinaryPath)
		}
		fmt.Println("endpoint:", status.BaseURL)
		return nil
	case "install":
		if status.BinaryFound {
			fmt.Println("Ollama is already installed:", status.BinaryPath)
			return nil
		}
		fmt.Print("Ollama was not found. Install the default Ollama backend now? [y/N] ")
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Println("Skipped Ollama install.")
			return nil
		}
		fmt.Println("Run ./scripts/install-ollama-linux.sh on Linux/WSL, brew install ollama on macOS, or install from https://ollama.com/download/windows on Windows.")
		return nil
	default:
		return fmt.Errorf("unknown ollama command %q", args[0])
	}
}

func withClient(ctx context.Context, opts cliOptions, fn func(*control.Client) error) error {
	c, err := app.ControlClient(opts.configPath)
	if err != nil {
		return err
	}
	_, cancel := context.WithTimeout(ctx, 24*time.Hour)
	defer cancel()
	return fn(c)
}

func printResponse(resp control.Response, err error, jsonOut bool) error {
	if err != nil {
		return err
	}
	if jsonOut {
		return printJSON(resp)
	}
	if !resp.OK {
		if resp.Error != nil {
			return errors.New(resp.Error.Message)
		}
		return errors.New("request failed")
	}
	return printJSON(resp.Data)
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printYAMLish(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func contains(xs []string, needle string) bool {
	for _, x := range xs {
		if x == needle {
			return true
		}
	}
	return false
}

func extractEvents(v any) []tasks.Event {
	raw, _ := json.Marshal(v)
	var payload struct {
		Events []tasks.Event `json:"events"`
	}
	_ = json.Unmarshal(raw, &payload)
	return payload.Events
}

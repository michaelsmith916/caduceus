package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/control"
)

const maxEnrollmentTokenBytes = 512

func enrollmentCommand(ctx context.Context, opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("enrollment command requires invite, list, get, approve, deny, audit, request, or status")
	}
	switch args[0] {
	case "invite":
		if len(args) != 1 {
			return errors.New("enrollment invite takes no arguments")
		}
		return withClient(ctx, opts, func(client *control.Client) error {
			response, err := client.CreateEnrollmentInvitation(ctx)
			return printResponse(response, err, opts.json)
		})
	case "list":
		if len(args) != 1 {
			return errors.New("enrollment list takes no arguments")
		}
		return withClient(ctx, opts, func(client *control.Client) error {
			response, err := client.ListEnrollmentRequests(ctx)
			return printResponse(response, err, opts.json)
		})
	case "get":
		if len(args) != 2 {
			return errors.New("enrollment get requires <request_id>")
		}
		return withClient(ctx, opts, func(client *control.Client) error {
			response, err := client.GetEnrollmentRequest(ctx, args[1])
			return printResponse(response, err, opts.json)
		})
	case "approve", "deny":
		return enrollmentDecisionCommand(ctx, opts, args[0], args[1:])
	case "audit":
		if len(args) != 1 {
			return errors.New("enrollment audit takes no arguments")
		}
		return withClient(ctx, opts, func(client *control.Client) error {
			response, err := client.EnrollmentAudit(ctx)
			return printResponse(response, err, opts.json)
		})
	case "request":
		return enrollmentRequestCommand(ctx, opts, args[1:])
	case "status":
		return enrollmentStatusCommand(ctx, opts, args[1:])
	default:
		return fmt.Errorf("unknown enrollment command %q", args[0])
	}
}

func enrollmentDecisionCommand(ctx context.Context, opts cliOptions, action string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("enrollment %s requires <request_id>", action)
	}
	requestID := args[0]
	flags := flag.NewFlagSet("enrollment "+action, flag.ContinueOnError)
	actor := flags.String("actor", "", "audit actor")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("enrollment %s received unexpected arguments", action)
	}
	return withClient(ctx, opts, func(client *control.Client) error {
		var response control.Response
		var err error
		if action == "approve" {
			response, err = client.ApproveEnrollment(ctx, requestID, *actor)
		} else {
			response, err = client.DenyEnrollment(ctx, requestID, *actor)
		}
		return printResponse(response, err, opts.json)
	})
}

func enrollmentRequestCommand(ctx context.Context, opts cliOptions, args []string) error {
	cfg, _, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	flags := flag.NewFlagSet("enrollment request", flag.ContinueOnError)
	coordinator := flags.String("coordinator", cfg.Enrollment.TrustedLAN.CoordinatorAddress, "coordinator /p2p/ multiaddress")
	tokenFile := flags.String("token-file", "", "file containing the one-time invitation token, or - for stdin")
	displayName := flags.String("name", cfg.Node.Name, "node display name")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("enrollment request received unexpected arguments")
	}
	if strings.TrimSpace(*coordinator) == "" {
		return errors.New("--coordinator is required when no coordinator_address is configured")
	}
	if strings.TrimSpace(*tokenFile) == "" {
		return errors.New("--token-file is required; use - to read the token from stdin")
	}
	token, err := readEnrollmentToken(*tokenFile, os.Stdin)
	if err != nil {
		return err
	}
	request := control.EnrollmentSubmitRequest{
		CoordinatorAddress: *coordinator,
		Token:              token,
		DisplayName:        *displayName,
	}
	return withClient(ctx, opts, func(client *control.Client) error {
		response, err := client.SubmitEnrollment(ctx, request)
		return printResponse(response, err, opts.json)
	})
}

func enrollmentStatusCommand(ctx context.Context, opts cliOptions, args []string) error {
	if len(args) == 0 {
		return errors.New("enrollment status requires <request_id>")
	}
	cfg, _, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	requestID := args[0]
	flags := flag.NewFlagSet("enrollment status", flag.ContinueOnError)
	coordinator := flags.String("coordinator", cfg.Enrollment.TrustedLAN.CoordinatorAddress, "coordinator /p2p/ multiaddress")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("enrollment status received unexpected arguments")
	}
	if strings.TrimSpace(*coordinator) == "" {
		return errors.New("--coordinator is required when no coordinator_address is configured")
	}
	request := control.EnrollmentStatusRequest{CoordinatorAddress: *coordinator, RequestID: requestID}
	return withClient(ctx, opts, func(client *control.Client) error {
		response, err := client.CheckEnrollment(ctx, request)
		return printResponse(response, err, opts.json)
	})
}

func readEnrollmentToken(path string, stdin io.Reader) (string, error) {
	var reader io.Reader
	var file *os.File
	if path == "-" {
		reader = stdin
	} else {
		opened, err := os.Open(path)
		if err != nil {
			return "", err
		}
		file = opened
		defer file.Close()
		reader = file
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxEnrollmentTokenBytes+2))
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if token == "" || len(token) > maxEnrollmentTokenBytes || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("token file must contain exactly one valid enrollment token")
	}
	return token, nil
}

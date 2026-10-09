package main

import (
	"context"
	"flag"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"io"
)

func isOnboardingCommand(command string) bool {
	switch command {
	case "enroll-prepare", "enroll-complete", "rotate-prepare", "rotate-complete":
		return true
	}
	return false
}
func runOnboarding(parent context.Context, arguments []string, output io.Writer) int {
	flags := flag.NewFlagSet(arguments[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	state := flags.String("state-dir", "", "explicit installation directory")
	pending := flags.String("pending-file", "", "private separate pending handoff")
	grantFile := flags.String("grant-file", "", "private one-time enrollment grant")
	setupFile := flags.String("setup-file", "", "private declarative setup")
	requestFile := flags.String("request-file", "", "private signed request output")
	receiptFile := flags.String("receipt-file", "", "private exact server receipt")
	headFile := flags.String("head-file", "", "private exact server worker head")
	invalid := func() int {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	if flags.Parse(arguments[1:]) != nil || flags.NArg() != 0 || *state == "" || *pending == "" {
		return invalid()
	}
	command := arguments[0]
	prepare := command == "enroll-prepare" || command == "rotate-prepare"
	if prepare {
		if *requestFile == "" || *receiptFile != "" {
			return invalid()
		}
	} else if *receiptFile == "" || *requestFile != "" || *headFile != "" || *grantFile != "" || *setupFile != "" {
		return invalid()
	}
	if command == "enroll-prepare" && (*grantFile == "" || *setupFile == "" || *headFile != "") || command == "rotate-prepare" && (*headFile == "" || *grantFile != "" || *setupFile != "") {
		return invalid()
	}
	store, e := attachedworkerlocal.NewStore(*state, nil)
	if e != nil {
		return invalid()
	}
	ctx, cancel := context.WithTimeout(parent, commandTimeout)
	defer cancel()
	switch command {
	case "enroll-prepare":
		grant, err := attachedworkeronboarding.ReadGrant(*grantFile)
		if err != nil {
			e = err
			break
		}
		setup, err := attachedworkeronboarding.ReadSetup(*setupFile)
		if err != nil {
			e = err
			break
		}
		claim, err := attachedworkeronboarding.PrepareEnrollment(ctx, store, *pending, grant, setup, nil)
		if err != nil {
			e = err
			break
		}
		e = attachedworkeronboarding.WriteClaim(*requestFile, claim)
	case "rotate-prepare":
		head, err := attachedworkeronboarding.ReadWorkerHead(*headFile)
		if err != nil {
			e = err
			break
		}
		rotation, err := attachedworkeronboarding.PrepareRotation(ctx, store, *pending, head, nil)
		if err != nil {
			e = err
			break
		}
		e = attachedworkeronboarding.WriteRotation(*requestFile, rotation)
	case "enroll-complete", "rotate-complete":
		receipt, err := attachedworkeronboarding.ReadReceipt(*receiptFile)
		if err != nil {
			e = err
			break
		}
		if command == "enroll-complete" {
			e = attachedworkeronboarding.CompleteEnrollment(ctx, store, *pending, receipt)
		} else {
			e = attachedworkeronboarding.CompleteRotation(ctx, store, *pending, receipt)
		}
	default:
		return invalid()
	}
	if e != nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: onboardingCode(e)}, 1)
	}
	return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeOK}, 0)
}
func onboardingCode(err error) attachedworkerlocal.ResultCode {
	switch err {
	case attachedworkeronboarding.ErrConflict:
		return attachedworkerlocal.CodeConflict
	case attachedworkeronboarding.ErrAmbiguous:
		return attachedworkerlocal.CodeAmbiguous
	case attachedworkeronboarding.ErrInvalid:
		return attachedworkerlocal.CodeInvalid
	}
	return attachedworkerlocal.Code(err)
}

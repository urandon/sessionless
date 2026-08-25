// Package idgen generates opaque, non-time-sortable identifiers for
// operational YDB keys.
package idgen

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitcode.com/urandon/sessionless/internal/ports"
)

const randomBytes = 16

var encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

type Generator struct {
	random io.Reader
}

func New() *Generator {
	return &Generator{random: rand.Reader}
}

func newWithReader(reader io.Reader) *Generator {
	return &Generator{random: reader}
}

func (generator *Generator) NewID(ctx context.Context, kind ports.IDKind) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	prefix, ok := prefixes[kind]
	if !ok {
		return "", fmt.Errorf("unsupported ID kind %q", kind)
	}
	if generator == nil || generator.random == nil {
		return "", errors.New("ID generator random source must not be nil")
	}
	var value [randomBytes]byte
	if _, err := io.ReadFull(generator.random, value[:]); err != nil {
		return "", fmt.Errorf("generate %s ID: %w", kind, err)
	}
	// The random component starts immediately after a fixed, kind-specific
	// prefix. It contains no timestamp or sequence and therefore has no moving
	// lexicographic edge inside a tenant key range.
	return prefix + strings.ToLower(encoding.EncodeToString(value[:])), nil
}

var prefixes = map[ports.IDKind]string{
	ports.IDTenant:                   "ten_",
	ports.IDUser:                     "usr_",
	ports.IDSession:                  "ses_",
	ports.IDSessionEvent:             "sev_",
	ports.IDFrontendBinding:          "fbd_",
	ports.IDSessionSnapshot:          "ssn_",
	ports.IDTenantInvitation:         "tiv_",
	ports.IDUploadIntent:             "upl_",
	ports.IDActor:                    "act_",
	ports.IDConversation:             "con_",
	ports.IDSubscriptionConnection:   "sub_",
	ports.IDRun:                      "run_",
	ports.IDAttempt:                  "att_",
	ports.IDLease:                    "lea_",
	ports.IDCheckpoint:               "chk_",
	ports.IDQuotaReservation:         "qrs_",
	ports.IDUsageObservation:         "use_",
	ports.IDArtifactManifest:         "art_",
	ports.IDDispatchOutbox:           "dsp_",
	ports.IDTelegramDelivery:         "tdl_",
	ports.IDQueueMessage:             "msg_",
	ports.IDCredentialHandle:         "crh_",
	ports.IDAttachedWorker:           "wrk_",
	ports.IDAttachedWorkerEnrollment: "wen_",
	ports.IDAttachedWorkerConnection: "wcn_",
	ports.IDAttachedWorkerChallenge:  "wch_",
}

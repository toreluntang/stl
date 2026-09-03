// Package blockevent owns the SQS block-event wire contract: decoding an SQS
// message body into a validated outbound.BlockEvent.
//
// The contract silently depends on the SNS subscription having
// RawMessageDelivery=true. Without it, SNS delivers a notification envelope
// ({"Type":"Notification","Message":"…",…}) which unmarshals into BlockEvent
// with err == nil and every field zero — and a zero ChainID was then deleted
// as "foreign chain". Decode rejects envelopes and zero-events as parse
// failures so callers leave them for redelivery/DLQ instead of deleting them.
//
// Full hash-shape validation (common.Hash typing) is follow-up work paired
// with the blockhash-equality cleanup; Decode currently requires only that a
// hash is present.
package blockevent

import (
	"encoding/json"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Decode strictly decodes an SQS message body into a BlockEvent.
func Decode(body []byte) (outbound.BlockEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return outbound.BlockEvent{}, fmt.Errorf("blockevent: malformed body: %w", err)
	}
	if isSNSEnvelope(raw) {
		return outbound.BlockEvent{}, fmt.Errorf("blockevent: SNS notification envelope (enable RawMessageDelivery=true on the subscription)")
	}

	var event outbound.BlockEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return outbound.BlockEvent{}, fmt.Errorf("blockevent: malformed body: %w", err)
	}
	if event.ChainID == 0 {
		return outbound.BlockEvent{}, fmt.Errorf("blockevent: missing chainId (is the subscription delivering raw messages?)")
	}
	if event.BlockHash == "" {
		return outbound.BlockEvent{}, fmt.Errorf("blockevent: missing blockHash")
	}
	return event, nil
}

// isSNSEnvelope reports whether the decoded object looks like an SNS
// notification envelope rather than a block event.
func isSNSEnvelope(raw map[string]json.RawMessage) bool {
	typeMsg, hasType := raw["Type"]
	_, hasMessage := raw["Message"]
	if !hasType || !hasMessage {
		return false
	}
	var typeStr string
	if err := json.Unmarshal(typeMsg, &typeStr); err != nil {
		return false
	}
	return typeStr == "Notification"
}

package openclaw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

const maxHistoryTurnLimit = 20

var _ pluginbridge.HistoryStreamer = (*Adapter)(nil)

// ReadHistoryStream subscribes before loading the initial page. Gateway events
// received during chat.history remain buffered by the RPC client and are
// reconciled after page_end, so the initial/live handoff cannot lose a turn.
func (a *Adapter) ReadHistoryStream(ctx context.Context, session pluginbridge.NativeSession, req pluginbridge.HistoryStreamRequest) (<-chan pluginbridge.HistoryStreamEvent, error) {
	sessionKey := strings.TrimSpace(session.NativeSessionID)
	if sessionKey == "" {
		return nil, fmt.Errorf("openclaw: native session id required")
	}
	if strings.TrimSpace(req.StreamID) == "" {
		req.StreamID = uuid.NewString()
	}
	req.Limit = boundedHistoryTurnLimit(req.Limit)

	client, err := a.dialClient(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := client.Request(ctx, "sessions.messages.subscribe", map[string]any{"key": sessionKey}); err != nil {
		_ = client.Close()
		return nil, err
	}

	out := make(chan pluginbridge.HistoryStreamEvent, 32)
	go a.runHistoryStream(ctx, client, sessionKey, req, out)
	return out, nil
}

func (a *Adapter) runHistoryStream(ctx context.Context, client rpcClient, sessionKey string, req pluginbridge.HistoryStreamRequest, out chan<- pluginbridge.HistoryStreamEvent) {
	defer close(out)
	defer client.Close()

	revisions := map[string]int64{}
	signatures := map[string]string{}
	turns, err := loadGatewayHistoryTurns(ctx, client, sessionKey, req.Limit)
	if err != nil {
		sendGatewayHistoryEvent(ctx, out, pluginbridge.HistoryStreamEvent{
			StreamID: req.StreamID, Type: "error", Source: "initial", Operation: "append",
			Error: err.Error(), Retryable: true,
		})
		return
	}
	for _, turn := range turns {
		revisions[turn.TurnID] = 1
		signatures[turn.TurnID] = gatewayHistoryTurnSignature(turn)
		turn.Revision = 1
		if !sendGatewayHistoryEvent(ctx, out, pluginbridge.HistoryStreamEvent{
			StreamID: req.StreamID, Type: "turn", Source: "initial", Operation: "append", Turn: &turn,
		}) {
			return
		}
	}
	terminalType := "end"
	if req.Live {
		terminalType = "page_end"
	}
	if !sendGatewayHistoryEvent(ctx, out, pluginbridge.HistoryStreamEvent{
		StreamID: req.StreamID, Type: terminalType, Source: "initial", Operation: "append",
	}) || !req.Live {
		return
	}

	for {
		frame, err := client.NextEventWithTimeout(ctx, 15*time.Minute)
		if err != nil {
			if ctx.Err() == nil {
				sendGatewayHistoryEvent(ctx, out, pluginbridge.HistoryStreamEvent{
					StreamID: req.StreamID, Type: "error", Source: "live", Operation: "replace",
					Error: err.Error(), Retryable: true,
				})
			}
			return
		}
		if !gatewayHistoryEventMatchesSession(frame, sessionKey) {
			continue
		}
		latest, err := loadGatewayHistoryTurns(ctx, client, sessionKey, req.Limit)
		if err != nil {
			sendGatewayHistoryEvent(ctx, out, pluginbridge.HistoryStreamEvent{
				StreamID: req.StreamID, Type: "error", Source: "live", Operation: "replace",
				Error: err.Error(), Retryable: true,
			})
			return
		}
		liveIDs := make(map[string]struct{}, len(latest))
		for _, turn := range latest {
			liveIDs[turn.TurnID] = struct{}{}
			signature := gatewayHistoryTurnSignature(turn)
			previous, exists := signatures[turn.TurnID]
			if exists && previous == signature {
				continue
			}
			operation := "append"
			if exists {
				operation = "replace"
			}
			revisions[turn.TurnID]++
			if revisions[turn.TurnID] == 0 {
				revisions[turn.TurnID] = 1
			}
			signatures[turn.TurnID] = signature
			turn.Revision = revisions[turn.TurnID]
			if !sendGatewayHistoryEvent(ctx, out, pluginbridge.HistoryStreamEvent{
				StreamID: req.StreamID, Type: "turn", Source: "live", Operation: operation, Turn: &turn,
			}) {
				return
			}
		}
		for turnID := range signatures {
			if _, exists := liveIDs[turnID]; !exists {
				delete(signatures, turnID)
				delete(revisions, turnID)
			}
		}
	}
}

func loadGatewayHistoryTurns(ctx context.Context, client rpcClient, sessionKey string, limit int) ([]pluginbridge.HistoryTurn, error) {
	messageLimit := boundedHistoryTurnLimit(limit) * 8
	if messageLimit < 50 {
		messageLimit = 50
	}
	if messageLimit > 200 {
		messageLimit = 200
	}
	payload, err := client.Request(ctx, "chat.history", chatHistoryParams(sessionKey, messageLimit))
	if err != nil {
		return nil, err
	}
	return gatewayHistoryTurns(payload["messages"], limit), nil
}

// gatewayHistoryTurns exposes only user/assistant pairs. OpenClaw may include
// orphan assistant, system, and tool rows; those are native-only and cannot be
// represented as a Prism turn without inventing a user message.
func gatewayHistoryTurns(value any, limit int) []pluginbridge.HistoryTurn {
	messages := historyMessagesFromGatewayPayload(value)
	turns := make([]pluginbridge.HistoryTurn, 0)
	var current *pluginbridge.HistoryTurn
	for _, message := range messages {
		switch strings.ToLower(strings.TrimSpace(message.Role)) {
		case "user":
			if current != nil {
				turns = append(turns, *current)
			}
			orderKey := message.CreatedAt.UTC().Format(time.RFC3339Nano)
			if message.CreatedAt.IsZero() || message.CreatedAt.Equal(time.Unix(0, 0).UTC()) {
				orderKey = message.ID
			}
			current = &pluginbridge.HistoryTurn{TurnID: message.ID, OrderKey: orderKey, Messages: []pluginbridge.HistoryMessage{message}}
		case "assistant":
			if current == nil {
				// Gateway root sessions can begin with a durable assistant reply.
				// Preserve that real message as its own turn instead of inventing a
				// user prompt or silently rendering an empty conversation.
				orderKey := message.CreatedAt.UTC().Format(time.RFC3339Nano)
				if message.CreatedAt.IsZero() || message.CreatedAt.Equal(time.Unix(0, 0).UTC()) {
					orderKey = message.ID
				}
				current = &pluginbridge.HistoryTurn{TurnID: message.ID, OrderKey: orderKey, Messages: []pluginbridge.HistoryMessage{message}}
			} else {
				current.Messages = append(current.Messages, message)
			}
		}
	}
	if current != nil {
		turns = append(turns, *current)
	}
	limit = boundedHistoryTurnLimit(limit)
	if len(turns) > limit {
		turns = turns[len(turns)-limit:]
	}
	for left, right := 0, len(turns)-1; left < right; left, right = left+1, right-1 {
		turns[left], turns[right] = turns[right], turns[left]
	}
	return turns
}

func boundedHistoryTurnLimit(value int) int {
	if value < 1 {
		return maxHistoryTurnLimit
	}
	if value > maxHistoryTurnLimit {
		return maxHistoryTurnLimit
	}
	return value
}

func gatewayHistoryTurnSignature(turn pluginbridge.HistoryTurn) string {
	b, _ := json.Marshal(turn.Messages)
	return string(b)
}

func gatewayHistoryEventMatchesSession(frame gatewayEventFrame, sessionKey string) bool {
	if frame.Event != "session.message" && frame.Event != "agent" {
		return false
	}
	observed := firstNonEmpty(stringFromAny(frame.Payload["sessionKey"]), stringFromAny(frame.Payload["key"]))
	if session, ok := frame.Payload["session"].(map[string]any); ok {
		observed = firstNonEmpty(observed, stringFromAny(session["key"]), stringFromAny(session["sessionKey"]))
	}
	return observed == "" || observed == sessionKey
}

func sendGatewayHistoryEvent(ctx context.Context, out chan<- pluginbridge.HistoryStreamEvent, event pluginbridge.HistoryStreamEvent) bool {
	select {
	case out <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func gatewayHistoryMessageID(message map[string]any, index int) string {
	native, _ := message["__openclaw"].(map[string]any)
	if id := firstNonEmpty(stringFromAny(message["id"]), stringFromAny(message["messageId"]), stringFromAny(native["id"])); id != "" {
		return "openclaw:" + id
	}
	b, _ := json.Marshal([]any{message["role"], message["timestamp"], message["createdAt"], message["content"], index})
	sum := sha256.Sum256(b)
	return "openclaw:" + hex.EncodeToString(sum[:12])
}

func gatewayHistoryMessageTime(message map[string]any) time.Time {
	native, _ := message["__openclaw"].(map[string]any)
	return firstNonZeroTime(
		timeFromAny(message["createdAt"]),
		timeFromAny(message["updatedAt"]),
		timeFromAny(message["timestamp"]),
		timeFromAny(native["recordTimestampMs"]),
	)
}

func gatewayHistoryMessageStatus(message map[string]any, role string) string {
	if status := strings.TrimSpace(stringFromAny(message["status"])); status != "" {
		return status
	}
	if strings.EqualFold(role, "assistant") && strings.EqualFold(stringFromAny(message["stopReason"]), "error") {
		return "failed"
	}
	return "completed"
}

package openclaw

import (
	"context"
	"testing"
	"time"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

func TestGatewayHistoryTurnsGroupsLatestUserTurns(t *testing.T) {
	payload := []any{
		gatewayHistoryFixture("assistant", "orphan", "a0", 1000),
		gatewayHistoryFixture("user", "first", "u1", 2000),
		gatewayHistoryFixture("assistant", "first answer", "a1", 3000),
		gatewayHistoryFixture("tool", "native only", "tool1", 3500),
		gatewayHistoryFixture("user", "second", "u2", 4000),
		gatewayHistoryFixture("assistant", "second answer", "a2", 5000),
		gatewayHistoryFixture("user", "third", "u3", 6000),
	}
	turns := gatewayHistoryTurns(payload, 2)
	if len(turns) != 2 {
		t.Fatalf("turn count = %d, want 2: %+v", len(turns), turns)
	}
	if turns[0].TurnID != "openclaw:u3" || len(turns[0].Messages) != 1 {
		t.Fatalf("latest turn = %+v", turns[0])
	}
	if turns[1].TurnID != "openclaw:u2" || len(turns[1].Messages) != 2 {
		t.Fatalf("older turn = %+v", turns[1])
	}
	if turns[1].Messages[1].ID != "openclaw:a2" {
		t.Fatalf("assistant stable id = %q", turns[1].Messages[1].ID)
	}
}

func TestGatewayHistoryTurnsKeepsLeadingAssistantWithoutInventingUser(t *testing.T) {
	turns := gatewayHistoryTurns([]any{
		gatewayHistoryFixture("assistant", "Gateway welcome", "assistant-root", 1000),
	}, 5)
	if len(turns) != 1 {
		t.Fatalf("turn count = %d, want 1: %+v", len(turns), turns)
	}
	turn := turns[0]
	if turn.TurnID != "openclaw:assistant-root" || len(turn.Messages) != 1 {
		t.Fatalf("unexpected orphan assistant turn: %+v", turn)
	}
	if turn.Messages[0].Role != "assistant" || turn.Messages[0].Content != "Gateway welcome" {
		t.Fatalf("assistant content was not preserved: %+v", turn.Messages[0])
	}
}

func TestReadHistoryStreamSubscribesBeforeInitialPage(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.messages.subscribe": {"ok": true},
			"chat.history": {"messages": []any{
				gatewayHistoryFixture("user", "hello", "u1", 1000),
				gatewayHistoryFixture("assistant", "world", "a1", 2000),
			}},
		},
		events: make(chan gatewayEventFrame),
	}
	adapter := adapterWithFake(fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := adapter.ReadHistoryStream(ctx, sessionFixture(), pluginbridge.HistoryStreamRequest{StreamID: "stream-1", Limit: 5, Live: true})
	if err != nil {
		t.Fatalf("ReadHistoryStream: %v", err)
	}
	turn := receiveHistoryEvent(t, stream)
	pageEnd := receiveHistoryEvent(t, stream)
	if turn.Type != "turn" || turn.Turn == nil || turn.Turn.TurnID != "openclaw:u1" || turn.Turn.Revision != 1 {
		t.Fatalf("initial turn = %+v", turn)
	}
	if pageEnd.Type != "page_end" {
		t.Fatalf("page end = %+v", pageEnd)
	}
	if len(fake.calls) < 2 || fake.calls[0].method != "sessions.messages.subscribe" || fake.calls[1].method != "chat.history" {
		t.Fatalf("call order = %+v", fake.calls)
	}
}

func gatewayHistoryFixture(role, text, id string, timestamp int64) map[string]any {
	return map[string]any{
		"role":       role,
		"content":    []any{map[string]any{"type": "text", "text": text}},
		"timestamp":  timestamp,
		"__openclaw": map[string]any{"id": id, "recordTimestampMs": timestamp, "seq": timestamp},
	}
}

func receiveHistoryEvent(t *testing.T, stream <-chan pluginbridge.HistoryStreamEvent) pluginbridge.HistoryStreamEvent {
	t.Helper()
	select {
	case event, ok := <-stream:
		if !ok {
			t.Fatal("history stream closed")
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("history stream event timed out")
		return pluginbridge.HistoryStreamEvent{}
	}
}

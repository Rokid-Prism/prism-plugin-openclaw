package openclaw

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

func TestDiscoverEndpointIgnoresStoredSessionEndpointByUsingCurrentDiscovery(t *testing.T) {
	adapter := New(Config{
		GatewayURL: "http://127.0.0.1:18789",
	})
	endpoint, detail, err := adapter.discoverEndpoint()
	if err != nil {
		t.Fatalf("discoverEndpoint: %v", err)
	}
	if endpoint != "http://127.0.0.1:18789" {
		t.Fatalf("expected configured endpoint, got %q", endpoint)
	}
	if detail == "" {
		t.Fatal("expected discovery detail")
	}
}

func TestProbePreservesUnverifiedDiscoveryReason(t *testing.T) {
	t.Setenv("OPENCLAW_GATEWAY_TOKEN", "test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	capability, err := New(Config{GatewayURL: "http://127.0.0.1:1"}).Probe(ctx)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if capability.Available {
		t.Fatalf("expected unavailable capability, got %+v", capability)
	}
	if !strings.Contains(capability.UnavailableReason, "gateway rpc auth failed") {
		t.Fatalf("expected diagnostic reason, got %q", capability.UnavailableReason)
	}
}

func TestGatewayAttachmentsEncodeMaterializedFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.txt")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	attachments, err := gatewayAttachments([]pluginbridge.Attachment{{
		Name: "sample.txt", MIMEType: "text/plain", LocalPath: path,
	}})
	if err != nil {
		t.Fatalf("gatewayAttachments: %v", err)
	}
	item := attachments[0].(map[string]any)
	if item["fileName"] != "sample.txt" || item["content"] != base64.StdEncoding.EncodeToString([]byte("hello")) {
		t.Fatalf("unexpected attachment: %+v", item)
	}
}

func TestGatewayAttachmentsRejectFilesAboveOpenClawDefaultLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.bin")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxGatewayAttachmentBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = gatewayAttachments([]pluginbridge.Attachment{{
		Name: "large.bin", MIMEType: "application/octet-stream", LocalPath: path,
	}})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size rejection, got %v", err)
	}
}

func TestFindMarkerEvidenceIncludesAttachmentNameWithoutFabricatingBodyText(t *testing.T) {
	content := []any{map[string]any{
		"role": "user",
		"content": []any{map[string]any{
			"type": "file", "fileName": "native-evidence.txt",
		}},
	}}
	evidence, ok := findMarkerEvidence(content, "native-evidence.txt")
	if !ok || !strings.Contains(evidence, "native-evidence.txt") {
		t.Fatalf("attachment marker not found: evidence=%q ok=%v", evidence, ok)
	}
	if text := flattenText(content); strings.Contains(text, "native-evidence.txt") {
		t.Fatalf("attachment name leaked into body text: %q", text)
	}
}

func TestChatHistoryParamsUseOnlyTheCurrentGatewaySchema(t *testing.T) {
	params := chatHistoryParams(" agent:main:prism:test ", 20)
	if params["sessionKey"] != "agent:main:prism:test" || params["limit"] != 20 || params["maxChars"] != 20000 {
		t.Fatalf("unexpected chat.history params: %+v", params)
	}
	if _, exists := params["sessionId"]; exists {
		t.Fatalf("chat.history must not receive the unsupported sessionId field: %+v", params)
	}
}

func TestGatewayAgentEndPhaseCompletesRun(t *testing.T) {
	event, ok := convertGatewayEvent("agent:main:prism:test", gatewayEventFrame{
		Type:  "event",
		Event: "agent",
		Seq:   7,
		Payload: map[string]any{
			"sessionKey": "agent:main:prism:test",
			"runId":      "run-7",
			"data": map[string]any{
				"phase": "end",
			},
		},
	})
	if !ok {
		t.Fatal("expected agent end event to be converted")
	}
	if event.Type != "run.completed" || event.Status != "completed" {
		t.Fatalf("expected phase=end to complete the run, got type=%q status=%q", event.Type, event.Status)
	}
}

func TestOpenClawSessionHintsReadMillisecondTimestampsAndPreserveGatewayRows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hints := openClawSessionHints("ws://127.0.0.1:18789", map[string]any{"sessions": []any{
		map[string]any{"key": "agent:main:prism:one", "label": "One", "updatedAt": now.UnixMilli()},
		map[string]any{"key": "global", "kind": "global", "updatedAt": now.UnixMilli(), "origin": map[string]any{"provider": "webchat"}},
		map[string]any{"key": "agent:main:cron:one", "kind": "cron", "spawnedBy": "cron:daily", "updatedAt": now.UnixMilli()},
	}})
	if len(hints) != 3 || hints[0].NativeSessionID != "agent:main:prism:one" || !hints[0].LastActivityAt.Equal(now) {
		t.Fatalf("unexpected hints: %+v", hints)
	}
	if hints[0].Metadata["agent_id"] != "main" {
		t.Fatalf("agent id was not derived from the session key: %+v", hints[0])
	}
	if hints[1].Metadata["session_kind"] != "global" || hints[1].Metadata["origin_provider"] != "webchat" {
		t.Fatalf("global session metadata was not preserved: %+v", hints[1])
	}
	if hints[2].Metadata["session_kind"] != "cron" || hints[2].Metadata["spawned_by"] != "cron:daily" {
		t.Fatalf("spawned session metadata was not preserved: %+v", hints[2])
	}
}

func TestOpenClawSessionHintNeverUsesGatewayKeyAsPlaceholderTitle(t *testing.T) {
	hint, ok := openClawSessionHint("ws://127.0.0.1:18789", map[string]any{
		"key":          "agent:main:main",
		"derivedTitle": "Untitled",
	})
	if !ok || hint.Title != "" {
		t.Fatalf("expected an empty title for a placeholder title, got %+v", hint)
	}

	renamed, ok := openClawSessionHint("ws://127.0.0.1:18789", map[string]any{
		"key":          "agent:main:main",
		"label":        "New Session",
		"derivedTitle": "Untitled",
	})
	if !ok || renamed.Title != "New Session" {
		t.Fatalf("expected user label to win, got %+v", renamed)
	}
}

func TestListSessionsReadsEveryGatewayPage(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"sessions.list|offset:0": {
			"sessions":   []any{map[string]any{"key": "agent:main:first", "kind": "direct"}},
			"hasMore":    true,
			"nextOffset": 1,
		},
		"sessions.list|offset:1": {
			"sessions": []any{map[string]any{"key": "agent:main:second", "kind": "direct"}},
			"hasMore":  false,
		},
	}}

	hints, err := listSessionsWithClient(context.Background(), "ws://127.0.0.1:18789", fake)
	if err != nil {
		t.Fatalf("listSessionsWithClient: %v", err)
	}
	if len(hints) != 2 || hints[0].NativeSessionID != "agent:main:first" || hints[1].NativeSessionID != "agent:main:second" {
		t.Fatalf("unexpected paginated hints: %+v", hints)
	}
	if len(fake.calls) != 2 || fake.calls[1].params["offset"] != 1 {
		t.Fatalf("expected two paginated calls, got %+v", fake.calls)
	}
	for _, key := range []string{"includeGlobal", "includeUnknown", "configuredAgentsOnly", "includeDerivedTitles"} {
		if fake.calls[0].params[key] != true {
			t.Fatalf("sessions.list must request %s: %+v", key, fake.calls[0].params)
		}
	}
}

func TestListSessionsRejectsBrokenPagination(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"sessions.list|offset:0": {
			"sessions":   []any{map[string]any{"key": "agent:main:first"}},
			"hasMore":    true,
			"nextOffset": 0,
		},
	}}

	_, err := listSessionsWithClient(context.Background(), "ws://127.0.0.1:18789", fake)
	if err == nil || !strings.Contains(err.Error(), "invalid nextOffset") {
		t.Fatalf("expected invalid pagination error, got %v", err)
	}
}

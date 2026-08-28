package openclaw

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

func TestLocalGatewayE2E(t *testing.T) {
	if os.Getenv("PRISM_OPENCLAW_E2E") != "1" {
		t.Skip("set PRISM_OPENCLAW_E2E=1 with a local OpenClaw Gateway running")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	a := New(Config{GatewayURL: "http://127.0.0.1:18789"})
	capability, err := a.Probe(ctx)
	if err != nil || !capability.Available {
		t.Fatalf("Probe capability=%+v err=%v", capability, err)
	}
	if sessions, err := a.ListSessions(ctx); err != nil || len(sessions) == 0 {
		t.Fatalf("ListSessions count=%d err=%v", len(sessions), err)
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	events, err := a.SubscribePlugin(watchCtx)
	if err != nil {
		t.Fatalf("SubscribePlugin: %v", err)
	}
	if event := receivePluginEvent(t, events); event.Type != "desktop.session.directory.reconciled" {
		t.Fatalf("initial watcher event: %+v", event)
	}

	key := fmt.Sprintf("agent:main:prism:e2e-%d", time.Now().UnixNano())
	client, err := a.dialClient(ctx)
	if err != nil {
		t.Fatalf("dialClient: %v", err)
	}
	if _, err := client.Request(ctx, "sessions.create", map[string]any{
		"key": key, "agentId": "main", "label": "Prism OpenClaw E2E",
	}); err != nil {
		client.Close()
		t.Fatalf("sessions.create: %v", err)
	}
	client.Close()
	defer func() {
		cleanup, cleanupErr := a.dialClient(context.Background())
		if cleanupErr == nil {
			_, _ = cleanup.Request(context.Background(), "sessions.delete", map[string]any{"key": key, "deleteTranscript": true})
			_ = cleanup.Close()
		}
	}()

	session := pluginbridge.NativeSession{
		PluginID: "openclaw", NativeSessionID: key, Surface: "gateway",
		Endpoint: "http://127.0.0.1:18789", Visible: true,
	}
	described, err := a.ControlSession(ctx, pluginbridge.ControlSessionRequest{Session: session, Action: "controls.describe"})
	if err != nil || len(described.Details) == 0 {
		t.Fatalf("controls.describe details=%+v err=%v", described.Details, err)
	}
	if _, exists := described.Details["permission_options"]; exists {
		t.Fatalf("OpenClaw must not publish unverifiable permission options: %+v", described.Details)
	}

	modelOptions, _ := described.Details["model_options"].([]any)
	if len(modelOptions) == 0 {
		t.Fatal("expected at least one available model")
	}
	modelKey := stringFromAny(modelOptions[0].(map[string]any)["key"])
	if _, err := a.ControlSession(ctx, pluginbridge.ControlSessionRequest{Session: session, Action: "model.switch", Target: modelKey}); err != nil {
		t.Fatalf("model.switch %s: %v", modelKey, err)
	}

	reasoningOptions, _ := described.Details["reasoning_options"].([]any)
	if len(reasoningOptions) > 0 {
		reasoningKey := stringFromAny(reasoningOptions[0].(map[string]any)["key"])
		if _, err := a.ControlSession(ctx, pluginbridge.ControlSessionRequest{Session: session, Action: "reasoning.switch", Target: reasoningKey}); err != nil {
			t.Fatalf("reasoning.switch %s: %v", reasoningKey, err)
		}
	}

	for _, control := range []pluginbridge.ControlSessionRequest{
		{Session: session, Action: "rename", Name: "Prism OpenClaw E2E Renamed"},
		{Session: session, Action: "pin", Target: map[string]any{"enabled": true}},
		{Session: session, Action: "context.compact"},
	} {
		result, err := a.ControlSession(ctx, control)
		if err != nil || !result.OK || !result.DetailsConfirmed {
			t.Fatalf("%s result=%+v err=%v", control.Action, result, err)
		}
	}

	foundIndex := false
	deadline := time.After(8 * time.Second)
	for !foundIndex {
		select {
		case event := <-events:
			if event.Type == "desktop.session.index.changed" && nativeSessionID(event) == key {
				foundIndex = true
			}
		case <-deadline:
			t.Fatal("wide watcher did not emit the E2E session index update")
		}
	}

	if result, err := a.ControlSession(ctx, pluginbridge.ControlSessionRequest{Session: session, Action: "archive"}); err != nil || !result.DetailsConfirmed {
		t.Fatalf("archive result=%+v err=%v", result, err)
	}
	if result, err := a.ControlSession(ctx, pluginbridge.ControlSessionRequest{Session: session, Action: "delete"}); err != nil || !result.DetailsConfirmed {
		t.Fatalf("delete result=%+v err=%v", result, err)
	}

	client, err = a.dialClient(ctx)
	if err != nil {
		t.Fatalf("dial after delete: %v", err)
	}
	defer client.Close()
	list, err := client.Request(ctx, "sessions.list", map[string]any{"limit": 200})
	if err != nil {
		t.Fatalf("sessions.list after delete: %v", err)
	}
	rows, _ := list["sessions"].([]any)
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		if stringFromAny(row["key"]) == key {
			t.Fatalf("deleted E2E session still present: %s", key)
		}
	}
}

func TestLocalGatewayPeerSyncE2E(t *testing.T) {
	if os.Getenv("PRISM_OPENCLAW_E2E") != "1" {
		t.Skip("set PRISM_OPENCLAW_E2E=1 with a local OpenClaw Gateway running")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	a := New(Config{GatewayURL: "http://127.0.0.1:18789"})

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	events, err := a.SubscribePlugin(watchCtx)
	if err != nil {
		t.Fatalf("SubscribePlugin: %v", err)
	}
	if event := receivePluginEvent(t, events); event.Type != "desktop.session.directory.reconciled" {
		t.Fatalf("initial watcher event: %+v", event)
	}

	peer, err := a.dialClient(ctx)
	if err != nil {
		t.Fatalf("dial peer client: %v", err)
	}
	defer peer.Close()
	stamp := time.Now().UnixNano()
	key := fmt.Sprintf("agent:main:prism:peer-e2e-%d", stamp)
	if _, err := peer.Request(ctx, "sessions.create", map[string]any{
		"key": key, "agentId": "main", "label": "Prism OpenClaw Peer E2E",
	}); err != nil {
		t.Fatalf("peer sessions.create: %v", err)
	}
	defer deleteE2ESession(t, a, key)

	index := receivePluginEventMatching(t, events, 8*time.Second, func(event pluginbridge.PluginEvent) bool {
		return event.Type == "desktop.session.index.changed" && nativeSessionID(event) == key
	})
	if index.Type == "" {
		t.Fatal("plugin watcher did not observe peer session creation")
	}

	peerMarker := fmt.Sprintf("PRISM_OPENCLAW_PEER_%d", stamp)
	peerRun, err := peer.Request(ctx, "chat.send", map[string]any{
		"sessionKey": key, "message": peerMarker, "deliver": false,
		"idempotencyKey": "peer-" + strconv.FormatInt(stamp, 10),
	})
	if err != nil {
		t.Fatalf("peer chat.send: %v", err)
	}
	peerEvent := receivePluginEventMatching(t, events, 10*time.Second, func(event pluginbridge.PluginEvent) bool {
		return event.Type == "message.user" && nativeSessionID(event) == key && strings.Contains(event.Summary, peerMarker)
	})
	if peerEvent.Type == "" {
		t.Fatal("plugin watcher did not observe the peer user message")
	}
	if runID := stringFromAny(peerRun["runId"]); runID != "" {
		_, _ = peer.Request(ctx, "chat.abort", map[string]any{"sessionKey": key, "runId": runID})
	}

	prismMarker := fmt.Sprintf("PRISM_OPENCLAW_PLUGIN_%d", stamp)
	receipt, err := a.Send(ctx, pluginbridge.NativeSession{
		PluginID: "openclaw", NativeSessionID: key, Surface: "gateway",
	}, pluginbridge.InboundMessage{
		PrismMessageID: "plugin-" + strconv.FormatInt(stamp, 10),
		Text:           prismMarker,
		SourceDevice:   "prism-openclaw-peer-e2e",
		Timestamp:      time.Now().UTC(),
	})
	if err != nil || !receipt.Accepted {
		t.Fatalf("plugin Send receipt=%+v err=%v", receipt, err)
	}
	defer func() {
		if receipt.NativeMessageID != "" {
			_, _ = peer.Request(context.Background(), "chat.abort", map[string]any{
				"sessionKey": key, "runId": receipt.NativeMessageID,
			})
		}
	}()

	deadline := time.Now().Add(8 * time.Second)
	for {
		history, historyErr := peer.Request(ctx, "chat.history", map[string]any{"sessionKey": key, "limit": 20})
		if historyErr == nil && strings.Contains(flattenVisibilityText(history["messages"]), prismMarker) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer history did not observe plugin message: history=%+v err=%v", history, historyErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestLocalGatewayConversationE2E(t *testing.T) {
	if os.Getenv("PRISM_OPENCLAW_E2E") != "1" {
		t.Skip("set PRISM_OPENCLAW_E2E=1 with a local OpenClaw Gateway running")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a := New(Config{GatewayURL: "http://127.0.0.1:18789"})

	stamp := time.Now().UnixNano()
	messageID := fmt.Sprintf("openclaw-conversation-e2e-%d", stamp)
	firstMarker := fmt.Sprintf("PRISM_OPENCLAW_FIRST_%d", stamp)
	attachmentPath := filepath.Join(t.TempDir(), "prism-openclaw-e2e.txt")
	if err := os.WriteFile(attachmentPath, []byte("PRISM_OPENCLAW_ATTACHMENT_OK\n"), 0o600); err != nil {
		t.Fatalf("write attachment: %v", err)
	}

	started, err := a.StartSessionWithMessage(ctx, pluginbridge.StartSessionWithMessageRequest{
		PluginID: "openclaw",
		Message: pluginbridge.InboundMessage{
			PrismMessageID: messageID,
			Text:           "Reply only " + firstMarker,
			Attachments: []pluginbridge.Attachment{{
				Name: "prism-openclaw-e2e.txt", MIMEType: "text/plain", LocalPath: attachmentPath,
			}},
			SourceDevice: "prism-openclaw-e2e",
			Timestamp:    time.Now().UTC(),
		},
	})
	if err != nil {
		t.Fatalf("StartSessionWithMessage: %v", err)
	}
	session := started.Session
	if !started.Receipt.Accepted || !started.Receipt.Visible || !started.Visibility.Visible {
		t.Fatalf("start receipt=%+v visibility=%+v", started.Receipt, started.Visibility)
	}
	defer deleteE2ESession(t, a, session.NativeSessionID)

	terminal, err := a.WaitForRun(ctx, session, started.Receipt.NativeMessageID)
	if err != nil {
		t.Fatalf("WaitForRun: %v", err)
	}
	if terminal.Type != "run.completed" && terminal.Type != "run.failed" {
		t.Fatalf("unexpected terminal event: %+v", terminal)
	}

	historyCtx, stopHistory := context.WithCancel(ctx)
	historyEvents, err := a.ReadHistoryStream(historyCtx, session, pluginbridge.HistoryStreamRequest{
		StreamID: "openclaw-conversation-e2e-history", Limit: 5, Live: true,
	})
	if err != nil {
		t.Fatalf("ReadHistoryStream: %v", err)
	}
	defer stopHistory()
	initialRendered := ""
	for {
		event := receiveHistoryEvent(t, historyEvents)
		if event.Turn != nil {
			initialRendered += fmt.Sprint(event.Turn)
		}
		if event.Type == "page_end" {
			break
		}
	}
	if !strings.Contains(initialRendered, firstMarker) {
		t.Fatalf("initial history stream does not contain first marker: %s", initialRendered)
	}

	secondMarker := fmt.Sprintf("PRISM_OPENCLAW_SECOND_%d", stamp)
	second, err := a.Send(ctx, session, pluginbridge.InboundMessage{
		PrismMessageID: messageID + "-second",
		Text:           "Reply only " + secondMarker,
		SourceDevice:   "prism-openclaw-e2e",
		Timestamp:      time.Now().UTC(),
	})
	if err != nil || !second.Accepted || !second.Visible {
		t.Fatalf("second Send receipt=%+v err=%v", second, err)
	}
	liveDeadline := time.After(15 * time.Second)
	for {
		select {
		case event, ok := <-historyEvents:
			if !ok {
				t.Fatal("live history stream closed before second marker")
			}
			if event.Type == "error" {
				t.Fatalf("live history stream failed: %+v", event)
			}
			if event.Source == "live" && event.Turn != nil && strings.Contains(fmt.Sprint(event.Turn), secondMarker) {
				stopHistory()
				goto liveHistoryObserved
			}
		case <-liveDeadline:
			t.Fatal("live history stream did not publish the second marker")
		}
	}

liveHistoryObserved:
	waitForHistoryStreamClose(t, historyEvents)
	visible, err := a.VerifyVisibility(ctx, session, secondMarker)
	if err != nil || !visible.Visible {
		t.Fatalf("second visibility=%+v err=%v", visible, err)
	}

	history, err := a.ReadHistory(ctx, session, 20)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	rendered := fmt.Sprint(history)
	if !strings.Contains(rendered, firstMarker) || !strings.Contains(rendered, secondMarker) {
		t.Fatalf("history does not contain both user markers: %s", rendered)
	}
	if strings.Contains(rendered, attachmentPath) {
		t.Fatalf("history leaked the materialized attachment path: %s", attachmentPath)
	}
}

func waitForHistoryStreamClose(t *testing.T, events <-chan pluginbridge.HistoryStreamEvent) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline.C:
			t.Fatal("history stream did not close after cancellation")
		}
	}
}

func receivePluginEventMatching(t *testing.T, events <-chan pluginbridge.PluginEvent, timeout time.Duration, match func(pluginbridge.PluginEvent) bool) pluginbridge.PluginEvent {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("plugin event stream closed before the expected event")
			}
			if match(event) {
				return event
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for matching plugin event")
			return pluginbridge.PluginEvent{}
		}
	}
}

func deleteE2ESession(t *testing.T, a *Adapter, sessionKey string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := a.dialClient(ctx)
	if err != nil {
		t.Errorf("cleanup dial: %v", err)
		return
	}
	defer client.Close()
	if _, err := client.Request(ctx, "sessions.delete", map[string]any{
		"key": sessionKey, "deleteTranscript": true,
	}); err != nil {
		t.Errorf("cleanup session %s: %v", sessionKey, err)
	}
}

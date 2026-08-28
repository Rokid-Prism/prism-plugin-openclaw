package openclaw

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

const defaultGatewayPort = 18789

const openClawSessionPageLimit = 200

// OpenClaw defaults agents.defaults.mediaMaxMb to 20. The Gateway remains the
// authority when a user configures a lower limit or a stricter media type.
const maxGatewayAttachmentBytes int64 = 20 * 1024 * 1024

type Adapter struct {
	Config Config
	dial   dialer
}

type Config struct {
	GatewayURL string
	HomeDir    string
	HTTPClient *http.Client
}

func New(config Config) *Adapter {
	return &Adapter{Config: config, dial: defaultDialer}
}

// defaultDialer wraps newGatewayClient so it satisfies the rpcClient interface.
func defaultDialer(ctx context.Context, cfg gatewayClientConfig) (rpcClient, error) {
	return newGatewayClient(ctx, cfg)
}

// dialClient resolves a connected rpcClient using the discovered endpoint and
// token. Centralizes the error handling for a missing token.
func (a *Adapter) dialClient(ctx context.Context) (rpcClient, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return nil, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("openclaw: gateway token not found")
	}
	dial := a.dial
	if dial == nil {
		dial = defaultDialer
	}
	return dial(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
}

func (a *Adapter) ID() string { return "openclaw" }

func (a *Adapter) Probe(ctx context.Context) (pluginbridge.Capability, error) {
	discovery, err := a.Discover(ctx)
	if err != nil {
		return pluginbridge.Capability{
			PluginID:           a.ID(),
			Available:          false,
			IntegrationMode:    pluginbridge.IntegrationModeProtocolNative,
			VisibilitySurface:  "gateway",
			UnavailableReason:  err.Error(),
			NativeVisibleInput: false,
			CanListSessions:    true,
			CanReadHistory:     true,
			CanForwardSync:     false, // gateway unreachable
			CanReverseSync:     true,
			CanPluginWideWatch: true,
			CanWaitRun:         true, // adapter implements RunWaiter
			CanReadStatus:      true,
		}, nil
	}
	available := discovery.Endpoint != "" && discovery.Verified
	return pluginbridge.Capability{
		PluginID:                   a.ID(),
		Available:                  available,
		IntegrationMode:            pluginbridge.IntegrationModeProtocolNative,
		NativeVisibleInput:         discovery.Endpoint != "" && discovery.Verified,
		NativeVisibleOutput:        discovery.Endpoint != "" && discovery.Verified,
		CanAttachSession:           true,
		CanStartSessionWithMessage: true,
		CanListSessions:            true,
		CanReadHistory:             true,
		CanInterrupt:               true,
		CanApproval:                true, // adapter implements ApprovalResolver
		CanForwardSync:             available,
		CanReverseSync:             true,
		CanPluginWideWatch:         true,
		CanWaitRun:                 true, // adapter implements RunWaiter
		CanReadStatus:              true, // adapter implements StatusReader
		CanControlSession:          true, // adapter implements SessionController
		VisibilitySurface:          "gateway",
		UnavailableReason:          unavailableDiscoveryReason(discovery, available),
	}, nil
}

func unavailableDiscoveryReason(discovery pluginbridge.DiscoveryResult, available bool) string {
	if available {
		return ""
	}
	if detail := strings.TrimSpace(discovery.Detail); detail != "" {
		return detail
	}
	return "openclaw gateway is not verified"
}

func (a *Adapter) Discover(ctx context.Context) (pluginbridge.DiscoveryResult, error) {
	endpoint, detail, err := a.discoverEndpoint()
	if err != nil {
		return pluginbridge.DiscoveryResult{PluginID: a.ID(), Surface: "gateway", Detail: detail}, err
	}
	healthOK := a.healthOK(ctx, endpoint)
	token, tokenSource, _ := a.discoverToken()
	if token == "" {
		return pluginbridge.DiscoveryResult{
			PluginID: a.ID(),
			Surface:  "gateway",
			Endpoint: endpoint,
			Verified: false,
			Detail:   joinDetails(detail, "gateway token not found"),
			SessionHints: map[string]string{
				"health": boolString(healthOK),
			},
		}, nil
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{
		Endpoint: endpoint,
		Token:    token,
	})
	if err != nil {
		return pluginbridge.DiscoveryResult{
			PluginID: a.ID(),
			Surface:  "gateway",
			Endpoint: endpoint,
			Verified: false,
			Detail:   joinDetails(detail, "gateway rpc auth failed: "+err.Error()),
			SessionHints: map[string]string{
				"health":      boolString(healthOK),
				"tokenSource": tokenSource,
			},
		}, nil
	}
	defer client.Close()
	if _, err := client.Request(ctx, "gateway.identity.get", map[string]any{}); err != nil {
		return pluginbridge.DiscoveryResult{
			PluginID: a.ID(),
			Surface:  "gateway",
			Endpoint: endpoint,
			Verified: false,
			Detail:   joinDetails(detail, "gateway identity failed: "+err.Error()),
			SessionHints: map[string]string{
				"health":      boolString(healthOK),
				"tokenSource": tokenSource,
			},
		}, nil
	}
	return pluginbridge.DiscoveryResult{
		PluginID: a.ID(),
		Surface:  "gateway",
		Endpoint: endpoint,
		Verified: true,
		Detail:   joinDetails(detail, "gateway rpc verified"),
		SessionHints: map[string]string{
			"health":      boolString(healthOK),
			"tokenSource": tokenSource,
		},
	}, nil
}

func (a *Adapter) discoverEndpoint() (string, string, error) {
	if url := strings.TrimSpace(a.Config.GatewayURL); url != "" && url != "auto" {
		return strings.TrimRight(url, "/"), "configured gateway_url", nil
	}
	root, err := openclawHome(a.Config.HomeDir)
	if err != nil {
		return "", "", err
	}
	if port := readGatewayPortFromConfig(filepath.Join(root, "openclaw.json")); port > 0 {
		return fmt.Sprintf("http://127.0.0.1:%d", port), "openclaw.json gateway.port", nil
	}
	if port := readGatewayPortFromEnv(filepath.Join(root, "service-env", "ai.openclaw.env")); port > 0 {
		return fmt.Sprintf("http://127.0.0.1:%d", port), "service-env OPENCLAW_GATEWAY_PORT", nil
	}
	return fmt.Sprintf("http://127.0.0.1:%d", defaultGatewayPort), "default openclaw gateway port", nil
}

func openclawHome(override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return filepath.Clean(strings.TrimSpace(override)), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", fmt.Errorf("openclaw: user home unavailable")
	}
	return filepath.Join(home, ".openclaw"), nil
}

func readGatewayPortFromConfig(path string) int {
	state, err := readOpenClawState(path)
	if err != nil {
		return 0
	}
	return state.Gateway.Port
}

func readGatewayPortFromEnv(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "export OPENCLAW_GATEWAY_PORT=") {
			continue
		}
		value := strings.TrimPrefix(line, "export OPENCLAW_GATEWAY_PORT=")
		value = strings.Trim(value, `'"`)
		port, _ := strconv.Atoi(value)
		return port
	}
	return 0
}

// StartSessionWithMessage is the only remote-new-conversation path. The
// gateway receives the stable first-message id both in its new session key and
// as the chat idempotency key, so a retry returns to the same native session.
func (a *Adapter) StartSessionWithMessage(ctx context.Context, req pluginbridge.StartSessionWithMessageRequest) (pluginbridge.StartSessionWithMessageResult, error) {
	messageID := strings.TrimSpace(req.Message.PrismMessageID)
	if messageID == "" || (strings.TrimSpace(req.Message.Text) == "" && len(req.Message.Attachments) == 0) {
		return pluginbridge.StartSessionWithMessageResult{}, fmt.Errorf("openclaw: first message id and content required")
	}
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return pluginbridge.StartSessionWithMessageResult{}, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return pluginbridge.StartSessionWithMessageResult{}, err
	}
	if token == "" {
		return pluginbridge.StartSessionWithMessageResult{}, fmt.Errorf("openclaw: gateway token not found")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return pluginbridge.StartSessionWithMessageResult{}, err
	}
	defer client.Close()
	sessionKey := defaultSessionKey(messageID)
	createParams := map[string]any{
		"key":     sessionKey,
		"agentId": "main",
		"label":   "Prism start " + messageID,
	}
	if cwd := strings.TrimSpace(req.Cwd); cwd != "" {
		createParams["cwd"] = cwd
	}
	payload, err := client.Request(ctx, "sessions.create", createParams)
	if err != nil {
		return pluginbridge.StartSessionWithMessageResult{}, err
	}
	session := pluginbridge.NativeSession{
		PluginID:        a.ID(),
		NativeSessionID: firstNonEmpty(stringFromMap(payload, "key"), sessionKey),
		NativeThreadID:  stringFromMap(payload, "sessionId"),
		Surface:         "gateway",
		Endpoint:        endpoint,
		Cwd:             req.Cwd,
		Visible:         true,
	}
	receipt, err := a.Send(ctx, session, req.Message)
	if err != nil {
		return pluginbridge.StartSessionWithMessageResult{}, err
	}
	visibilityMarker := strings.TrimSpace(req.Message.Text)
	if visibilityMarker == "" && len(req.Message.Attachments) > 0 {
		visibilityMarker = firstNonEmpty(req.Message.Attachments[0].Name, filepath.Base(req.Message.Attachments[0].LocalPath))
	}
	visibility, err := a.VerifyVisibility(ctx, session, visibilityMarker)
	if err != nil {
		return pluginbridge.StartSessionWithMessageResult{}, err
	}
	return pluginbridge.StartSessionWithMessageResult{Session: session, Receipt: receipt, Visibility: visibility}, nil
}

func (a *Adapter) AttachSession(ctx context.Context, req pluginbridge.AttachSessionRequest) (pluginbridge.NativeSession, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return pluginbridge.NativeSession{}, err
	}
	nativeSessionID := strings.TrimSpace(req.NativeSessionID)
	if nativeSessionID == "" {
		return pluginbridge.NativeSession{}, fmt.Errorf("openclaw: native session id required")
	}
	return pluginbridge.NativeSession{
		PluginID:        a.ID(),
		NativeSessionID: nativeSessionID,
		NativeThreadID:  strings.TrimSpace(req.NativeThreadID),
		Surface:         "gateway",
		Endpoint:        endpoint,
		Cwd:             req.Cwd,
		Visible:         true,
	}, nil
}

func (a *Adapter) ListSessions(ctx context.Context) ([]pluginbridge.NativeSessionHint, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return nil, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("openclaw: gateway token not found")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return listSessionsWithClient(ctx, endpoint, client)
}

func listSessionsWithClient(ctx context.Context, endpoint string, client rpcClient) ([]pluginbridge.NativeSessionHint, error) {
	payload, err := listGatewaySessionPages(ctx, client)
	if err == nil {
		return openClawSessionHints(endpoint, payload), nil
	}
	if !isUnsupportedGatewayMethod(err) {
		return nil, err
	}

	// sessions.active predates the paginated directory contract. Keep it only
	// as a compatibility path for gateways that do not implement sessions.list.
	payload, fallbackErr := client.Request(ctx, "sessions.active", map[string]any{})
	if fallbackErr == nil {
		return openClawSessionHints(endpoint, payload), nil
	}
	if isUnsupportedGatewayMethod(fallbackErr) {
		return nil, nil
	}
	return nil, fallbackErr
}

func listGatewaySessionPages(ctx context.Context, client rpcClient) (map[string]any, error) {
	all := make([]any, 0, openClawSessionPageLimit)
	offset := 0
	seenOffsets := map[int]struct{}{}
	for {
		if _, seen := seenOffsets[offset]; seen {
			return nil, fmt.Errorf("openclaw: sessions.list repeated offset %d", offset)
		}
		seenOffsets[offset] = struct{}{}

		params := map[string]any{
			"limit":                openClawSessionPageLimit,
			"includeGlobal":        true,
			"includeUnknown":       true,
			"configuredAgentsOnly": true,
			"includeDerivedTitles": true,
		}
		if offset > 0 {
			params["offset"] = offset
		}
		payload, err := client.Request(ctx, "sessions.list", params)
		if err != nil {
			return nil, err
		}
		rows := firstArray(payload, "sessions", "items", "data")
		all = append(all, rows...)
		if !boolFromAny(payload["hasMore"]) {
			return map[string]any{"sessions": all}, nil
		}
		nextNumber, ok := numberFromAny(payload["nextOffset"])
		nextOffset := int(nextNumber)
		if !ok || nextOffset <= offset {
			return nil, fmt.Errorf("openclaw: sessions.list returned invalid nextOffset %v after offset %d", payload["nextOffset"], offset)
		}
		offset = nextOffset
	}
}

func (a *Adapter) Send(ctx context.Context, session pluginbridge.NativeSession, msg pluginbridge.InboundMessage) (pluginbridge.SendReceipt, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return pluginbridge.SendReceipt{}, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return pluginbridge.SendReceipt{}, err
	}
	if token == "" {
		return pluginbridge.SendReceipt{}, fmt.Errorf("openclaw: gateway token not found")
	}
	sessionKey := strings.TrimSpace(session.NativeSessionID)
	if sessionKey == "" {
		return pluginbridge.SendReceipt{}, fmt.Errorf("openclaw: native session id required")
	}
	text := strings.TrimSpace(msg.Text)
	if text == "" && len(msg.Attachments) == 0 {
		return pluginbridge.SendReceipt{}, fmt.Errorf("openclaw: message content required")
	}
	messageID := strings.TrimSpace(msg.PrismMessageID)
	if messageID == "" {
		messageID = uuid.NewString()
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return pluginbridge.SendReceipt{}, err
	}
	defer client.Close()
	params := map[string]any{
		"sessionKey":     sessionKey,
		"message":        text,
		"deliver":        false,
		"idempotencyKey": messageID,
	}
	attachments, err := gatewayAttachments(msg.Attachments)
	if err != nil {
		return pluginbridge.SendReceipt{}, err
	}
	if len(attachments) > 0 {
		params["attachments"] = attachments
	}
	if session.NativeThreadID != "" {
		params["sessionId"] = session.NativeThreadID
	}
	payload, err := client.Request(ctx, "chat.send", params)
	if err != nil {
		return pluginbridge.SendReceipt{}, err
	}
	runID := firstNonEmpty(stringFromMap(payload, "runId"), messageID)
	detail := strings.TrimSpace(stringFromMap(payload, "status"))
	return pluginbridge.SendReceipt{
		NativeMessageID: runID,
		Accepted:        true,
		Visible:         true,
		Detail:          detail,
	}, nil
}

func (a *Adapter) Subscribe(ctx context.Context, session pluginbridge.NativeSession) (<-chan pluginbridge.PluginEvent, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return nil, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("openclaw: gateway token not found")
	}
	sessionKey := strings.TrimSpace(session.NativeSessionID)
	if sessionKey == "" {
		return nil, fmt.Errorf("openclaw: native session id required")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return nil, err
	}
	if _, err := client.Request(ctx, "sessions.messages.subscribe", map[string]any{"key": sessionKey}); err != nil {
		_ = client.Close()
		return nil, err
	}
	out := make(chan pluginbridge.PluginEvent, 32)
	go func() {
		defer close(out)
		defer client.Close()
		for {
			event, err := client.NextEventWithTimeout(ctx, 15*time.Minute)
			if err != nil {
				if ctx.Err() == nil {
					sendPluginEvent(ctx, out, pluginbridge.PluginEvent{
						ID:        uuid.NewString(),
						Type:      "subscription.error",
						Status:    "failed",
						Summary:   err.Error(),
						CreatedAt: time.Now().UTC(),
					})
				}
				return
			}
			converted, ok := convertGatewayEvent(sessionKey, event)
			if !ok {
				continue
			}
			// On agent lifecycle events (run started/ended/failed) attach a fresh
			// detail_snapshot carrying the live run state, so the Hub/mobile sees
			// the current model/reasoning/permission/options AND the run phase/
			// preview/status together. Message events do not rebuild the snapshot.
			if event.Event == "agent" {
				data, _ := event.Payload["data"].(map[string]any)
				run := runFromAgentEvent(data)
				if snap, err := a.buildDetailSnapshotWithRun(ctx, client, session, run); err == nil {
					converted = withDetailSnapshot(converted, snap)
				}
			}
			if !sendPluginEvent(ctx, out, converted) {
				return
			}
		}
	}()
	return out, nil
}

// SubscribePlugin uses OpenClaw's broad session subscription. One Gateway
// connection carries directory, message, run, and approval events for every
// session, which matches Hub's plugin-wide watcher contract without opening a
// websocket per conversation.
func (a *Adapter) SubscribePlugin(ctx context.Context) (<-chan pluginbridge.PluginEvent, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return nil, err
	}
	client, err := a.dialClient(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := client.Request(ctx, "sessions.subscribe", map[string]any{}); err != nil {
		_ = client.Close()
		return nil, err
	}

	out := make(chan pluginbridge.PluginEvent, 128)
	go func() {
		defer close(out)
		defer client.Close()
		if !sendPluginEvent(ctx, out, pluginbridge.PluginEvent{
			ID:        "openclaw-directory-initial-" + uuid.NewString(),
			Type:      "desktop.session.directory.reconciled",
			Status:    "completed",
			Summary:   "OpenClaw 会话目录已连接。",
			CreatedAt: time.Now().UTC(),
			Payload:   map[string]any{"source": "openclaw-gateway"},
		}) {
			return
		}

		for _, pending := range pendingApprovalRecords(ctx, client) {
			event, ok := a.approvalRequiredEvent(ctx, client, endpoint, pending.kind, pending.approval)
			if ok && !sendPluginEvent(ctx, out, event) {
				return
			}
		}

		for {
			frame, err := client.NextEventWithTimeout(ctx, 15*time.Minute)
			if err != nil {
				if ctx.Err() == nil {
					sendPluginEvent(ctx, out, pluginbridge.PluginEvent{
						ID:        uuid.NewString(),
						Type:      "subscription.error",
						Status:    "failed",
						Summary:   err.Error(),
						CreatedAt: time.Now().UTC(),
					})
				}
				return
			}

			var event pluginbridge.PluginEvent
			var ok bool
			switch frame.Event {
			case "sessions.changed":
				event, ok = sessionIndexEvent(endpoint, frame)
			case "session.message", "agent":
				event, ok = convertGatewayEvent("", frame)
				if ok {
					session := nativeSessionForGatewayEvent(endpoint, frame)
					if session.NativeSessionID == "" {
						ok = false
					} else {
						event = attachNativeSession(event, session)
						if frame.Event == "agent" {
							data, _ := frame.Payload["data"].(map[string]any)
							run := runFromAgentEvent(data)
							if snap, snapErr := a.buildDetailSnapshotWithRun(ctx, client, session, run); snapErr == nil {
								event = withDetailSnapshot(event, snap)
							}
						}
					}
				}
			case "exec.approval.requested", "plugin.approval.requested":
				kind := strings.TrimSuffix(frame.Event, ".approval.requested")
				event, ok = a.approvalRequiredEvent(ctx, client, endpoint, kind, frame.Payload)
			case "exec.approval.resolved", "plugin.approval.resolved":
				kind := strings.TrimSuffix(frame.Event, ".approval.resolved")
				event, ok = approvalResolvedEvent(endpoint, kind, frame.Payload)
			}
			if ok && !sendPluginEvent(ctx, out, event) {
				return
			}
		}
	}()
	return out, nil
}

func (a *Adapter) WaitForRun(ctx context.Context, session pluginbridge.NativeSession, runID string) (pluginbridge.PluginEvent, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return pluginbridge.PluginEvent{}, fmt.Errorf("openclaw: run id required")
	}
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return pluginbridge.PluginEvent{}, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return pluginbridge.PluginEvent{}, err
	}
	if token == "" {
		return pluginbridge.PluginEvent{}, fmt.Errorf("openclaw: gateway token not found")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return pluginbridge.PluginEvent{}, err
	}
	defer client.Close()
	waitTimeout := 10 * time.Minute
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			waitTimeout = remaining
		}
	}
	payload, err := client.RequestWithTimeout(ctx, "agent.wait", map[string]any{
		"runId":     runID,
		"timeoutMs": int(waitTimeout / time.Millisecond),
	}, waitTimeout+2*time.Second)
	if err != nil {
		return pluginbridge.PluginEvent{}, err
	}
	status := normalizeWaitStatus(stringFromMap(payload, "status"))
	summary, _ := a.latestAssistantSummary(ctx, session, 600)
	if summary == "" {
		switch status {
		case "completed":
			summary = "OpenClaw 已完成。"
		case "failed":
			summary = "OpenClaw 执行失败。"
		default:
			summary = "OpenClaw 状态：" + status
		}
	}
	eventType := "run.completed"
	if status == "failed" {
		eventType = "run.failed"
	}
	event := pluginbridge.PluginEvent{
		ID:        runID,
		Type:      eventType,
		Status:    status,
		Summary:   summary,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}
	// Attach a terminal detail_snapshot carrying the post-run state (terminal
	// status/summary plus refreshed model/reasoning/permission/context). The
	// client is still connected here.
	run := runFromWaitStatus(payload)
	if snap, err := a.buildDetailSnapshotWithRun(ctx, client, session, run); err == nil {
		event = withDetailSnapshot(event, snap)
	}
	return event, nil
}

// ReadStatus performs a non-blocking run check. agent.wait with timeoutMs=0
// returns status=timeout while a run is still active; that is a liveness result,
// not a terminal timeout.
func (a *Adapter) ReadStatus(ctx context.Context, session pluginbridge.NativeSession, runID string) (pluginbridge.RunStatus, error) {
	client, err := a.dialClient(ctx)
	if err != nil {
		return pluginbridge.RunStatus{}, err
	}
	defer client.Close()

	status := pluginbridge.RunStatus{Status: "idle", PrimaryAction: "send"}
	if runID = strings.TrimSpace(runID); runID != "" {
		payload, err := client.RequestWithTimeout(ctx, "agent.wait", map[string]any{
			"runId": runID, "timeoutMs": 0,
		}, 2*time.Second)
		if err != nil {
			return pluginbridge.RunStatus{}, err
		}
		nativeStatus := strings.ToLower(stringFromAny(payload["status"]))
		if nativeStatus == "timeout" {
			status.Status = "running"
			status.Interruptible = true
			status.PrimaryAction = "interrupt"
		} else {
			status.Status = normalizeWaitStatus(nativeStatus)
		}
		status.TurnID = runID
		status.StartedAt = gatewayTimestamp(payload["startedAt"])
		status.CompletedAt = gatewayTimestamp(payload["endedAt"])
	}

	row, describeErr := a.sessionDescribe(ctx, client, session.NativeSessionID)
	if describeErr == nil && row != nil {
		if runID == "" && (boolFromAny(row["hasActiveRun"]) || strings.EqualFold(stringFromAny(row["status"]), "running")) {
			status.Status = "running"
			status.Interruptible = true
			status.PrimaryAction = "interrupt"
		}
		projection := map[string]any{}
		applySessionRow(projection, row)
		status.Model = projection["current_model"]
		status.ReasoningMode = projection["current_reasoning"]
		if used, ok := projection["context_tokens_used"]; ok {
			status.Context = map[string]any{"tokens_used": used}
			if total, exists := projection["context_window_total"]; exists {
				status.Context["window_total"] = total
			}
		}
	}
	return status, nil
}

func (a *Adapter) Interrupt(ctx context.Context, session pluginbridge.NativeSession, taskID string) error {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("openclaw: gateway token not found")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return err
	}
	defer client.Close()
	params := map[string]any{"sessionKey": session.NativeSessionID}
	if strings.TrimSpace(taskID) != "" {
		params["runId"] = strings.TrimSpace(taskID)
	}
	_, err = client.Request(ctx, "chat.abort", params)
	return err
}

func (a *Adapter) latestAssistantSummary(ctx context.Context, session pluginbridge.NativeSession, maxChars int) (string, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return "", err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("openclaw: gateway token not found")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return "", err
	}
	defer client.Close()
	payload, err := client.Request(ctx, "chat.history", chatHistoryParams(session.NativeSessionID, 20))
	if err != nil {
		return "", err
	}
	text := latestAssistantText(payload["messages"])
	return trimRunes(strings.Join(strings.Fields(text), " "), maxChars), nil
}

func (a *Adapter) ReadHistory(ctx context.Context, session pluginbridge.NativeSession, limit int) ([]pluginbridge.HistoryMessage, error) {
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return nil, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("openclaw: gateway token not found")
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	params := chatHistoryParams(session.NativeSessionID, limitOrDefault(limit, 200))
	payload, err := client.Request(ctx, "chat.history", params)
	if err != nil {
		return nil, err
	}
	return historyMessagesFromGatewayPayload(payload["messages"]), nil
}

func (a *Adapter) VerifyVisibility(ctx context.Context, session pluginbridge.NativeSession, marker string) (pluginbridge.VisibilityResult, error) {
	checkedAt := time.Now().UTC()
	marker = strings.TrimSpace(marker)
	if marker == "" {
		return pluginbridge.VisibilityResult{CheckedAt: checkedAt, FailureReason: "marker required"}, nil
	}
	endpoint, _, err := a.discoverEndpoint()
	if err != nil {
		return pluginbridge.VisibilityResult{Marker: marker, CheckedAt: checkedAt, FailureReason: err.Error()}, err
	}
	token, _, err := a.discoverToken()
	if err != nil {
		return pluginbridge.VisibilityResult{Marker: marker, CheckedAt: checkedAt, FailureReason: err.Error()}, err
	}
	if token == "" {
		return pluginbridge.VisibilityResult{Marker: marker, CheckedAt: checkedAt, FailureReason: "gateway token not found"}, nil
	}
	client, err := newGatewayClient(ctx, gatewayClientConfig{Endpoint: endpoint, Token: token})
	if err != nil {
		return pluginbridge.VisibilityResult{Marker: marker, CheckedAt: checkedAt, FailureReason: err.Error()}, err
	}
	defer client.Close()
	deadline := time.Now().Add(15 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline
	}
	for {
		payload, err := client.Request(ctx, "chat.history", chatHistoryParams(session.NativeSessionID, 50))
		checkedAt = time.Now().UTC()
		if err != nil {
			return pluginbridge.VisibilityResult{Marker: marker, CheckedAt: checkedAt, FailureReason: err.Error()}, err
		}
		evidence, ok := findMarkerEvidence(payload["messages"], marker)
		if ok {
			return pluginbridge.VisibilityResult{
				Visible:   true,
				Marker:    marker,
				Evidence:  evidence,
				CheckedAt: checkedAt,
			}, nil
		}
		if time.Now().After(deadline) {
			return pluginbridge.VisibilityResult{
				Visible:       false,
				Marker:        marker,
				CheckedAt:     checkedAt,
				FailureReason: "marker not found in chat.history",
			}, nil
		}
		select {
		case <-ctx.Done():
			return pluginbridge.VisibilityResult{Marker: marker, CheckedAt: checkedAt, FailureReason: ctx.Err().Error()}, ctx.Err()
		case <-time.After(750 * time.Millisecond):
		}
	}
}

func chatHistoryParams(sessionKey string, limit int) map[string]any {
	return map[string]any{
		"sessionKey": strings.TrimSpace(sessionKey),
		"limit":      limit,
		"maxChars":   20000,
	}
}

func (a *Adapter) Close(context.Context) error { return nil }

func (a *Adapter) healthOK(ctx context.Context, endpoint string) bool {
	client := a.Config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 1500 * time.Millisecond}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (a *Adapter) discoverToken() (string, string, error) {
	if token := strings.TrimSpace(os.Getenv("OPENCLAW_GATEWAY_TOKEN")); token != "" {
		return token, "env OPENCLAW_GATEWAY_TOKEN", nil
	}
	root, err := openclawHome(a.Config.HomeDir)
	if err != nil {
		return "", "", err
	}
	token := readGatewayTokenFromConfig(filepath.Join(root, "openclaw.json"))
	if token != "" {
		return token, "openclaw.json gateway.auth.token", nil
	}
	return "", "", nil
}

type openClawState struct {
	Gateway struct {
		Port int `json:"port"`
		Auth struct {
			Token string `json:"token"`
		} `json:"auth"`
	} `json:"gateway"`
}

func readOpenClawState(path string) (openClawState, error) {
	var raw openClawState
	b, err := os.ReadFile(path)
	if err != nil {
		return raw, err
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return raw, err
	}
	return raw, nil
}

func readGatewayTokenFromConfig(path string) string {
	state, err := readOpenClawState(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(state.Gateway.Auth.Token)
}

func defaultSessionKey(conversationID string) string {
	id := strings.TrimSpace(conversationID)
	if id == "" {
		id = uuid.NewString()
	}
	return "agent:main:prism:" + sanitizeSessionKeyPart(id)
}

func gatewayAttachments(input []pluginbridge.Attachment) ([]any, error) {
	attachments := make([]any, 0, len(input))
	for index, attachment := range input {
		localPath := strings.TrimSpace(attachment.LocalPath)
		if localPath == "" {
			return nil, fmt.Errorf("openclaw: attachment %d local path required", index+1)
		}
		info, err := os.Stat(localPath)
		if err != nil {
			return nil, fmt.Errorf("openclaw: stat attachment %d: %w", index+1, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("openclaw: attachment %d is not a regular file", index+1)
		}
		if info.Size() > maxGatewayAttachmentBytes {
			return nil, fmt.Errorf("openclaw: attachment %d exceeds %d byte limit", index+1, maxGatewayAttachmentBytes)
		}
		content, err := os.ReadFile(localPath)
		if err != nil {
			return nil, fmt.Errorf("openclaw: read attachment %d: %w", index+1, err)
		}
		name := strings.TrimSpace(attachment.Name)
		if name == "" {
			name = filepath.Base(localPath)
		}
		mimeType := strings.TrimSpace(attachment.MIMEType)
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		kind := "file"
		if strings.HasPrefix(strings.ToLower(mimeType), "image/") {
			kind = "image"
		}
		attachments = append(attachments, map[string]any{
			"type":     kind,
			"mimeType": mimeType,
			"fileName": name,
			"content":  base64.StdEncoding.EncodeToString(content),
		})
	}
	return attachments, nil
}

func openClawSessionHints(endpoint string, payload map[string]any) []pluginbridge.NativeSessionHint {
	values := firstArray(payload, "sessions", "items", "data")
	out := make([]pluginbridge.NativeSessionHint, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, item := range values {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		hint, ok := openClawSessionHint(endpoint, m)
		if !ok {
			continue
		}
		if _, exists := seen[hint.NativeSessionID]; exists {
			continue
		}
		seen[hint.NativeSessionID] = struct{}{}
		out = append(out, hint)
	}
	return out
}

func openClawSessionHint(endpoint string, m map[string]any) (pluginbridge.NativeSessionHint, bool) {
	key := firstNonEmpty(
		stringFromAny(m["key"]),
		stringFromAny(m["sessionKey"]),
		stringFromAny(m["id"]),
	)
	if key == "" {
		return pluginbridge.NativeSessionHint{}, false
	}
	kind := strings.ToLower(strings.TrimSpace(stringFromAny(m["kind"])))
	metadata := map[string]string{}
	if strings.HasPrefix(key, "agent:main:prism:") {
		metadata["prism_managed"] = "true"
	}
	if kind != "" {
		metadata["session_kind"] = kind
	}
	if boolFromAny(m["pinned"]) {
		metadata["pinned"] = "true"
	}
	if boolFromAny(m["unread"]) {
		metadata["unread"] = "true"
	}
	if boolFromAny(m["archived"]) {
		metadata["archived"] = "true"
	}
	for source, target := range map[string]string{
		"category":  "category",
		"spawnedBy": "spawned_by",
		"chatType":  "chat_type",
	} {
		if value := stringFromAny(m[source]); value != "" {
			metadata[target] = value
		}
	}
	if agentID := firstNonEmpty(stringFromAny(m["agentId"]), openClawAgentIDFromSessionKey(key)); agentID != "" {
		// Native agent identity remains adapter-internal. The unified index
		// allowlist deliberately does not expose agent_id on the public wire.
		metadata["agent_id"] = agentID
	}
	if origin, ok := m["origin"].(map[string]any); ok {
		for source, target := range map[string]string{
			"provider": "origin_provider",
			"surface":  "origin_surface",
			"chatType": "origin_chat_type",
		} {
			if value := stringFromAny(origin[source]); value != "" {
				metadata[target] = value
			}
		}
	}
	status := strings.TrimSpace(stringFromAny(m["status"]))
	if status == "" {
		if _, declared := m["hasActiveRun"]; declared {
			status = map[bool]string{true: "running", false: "idle"}[boolFromAny(m["hasActiveRun"])]
		}
	}
	if status != "" {
		metadata["status"] = status
	}
	updated := timeFromAny(firstNonNil(m["updatedAt"], m["lastActivityAt"]))
	if !updated.IsZero() {
		metadata["sort_at"] = strconv.FormatInt(updated.UnixMilli(), 10)
	}
	prismConversationID := firstNonEmpty(
		stringFromAny(m["prismConversationId"]),
		stringFromAny(m["prism_conversation_id"]),
	)
	active := boolFromAny(m["active"]) || boolFromAny(m["hasActiveRun"]) || strings.EqualFold(stringFromAny(m["status"]), "running")
	return pluginbridge.NativeSessionHint{
		PluginID:            "openclaw",
		NativeSessionID:     key,
		NativeThreadID:      stringFromAny(m["sessionId"]),
		Surface:             "gateway",
		Endpoint:            endpoint,
		Cwd:                 firstNonEmpty(stringFromAny(m["cwd"]), stringFromAny(m["spawnedCwd"]), stringFromAny(m["execCwd"])),
		Title:               openClawSessionTitle(key, m),
		PrismConversationID: prismConversationID,
		Active:              active,
		Visible:             true,
		LastActivityAt:      updated,
		Metadata:            metadata,
	}, true
}

func openClawSessionTitle(key string, row map[string]any) string {
	// A user-provided label is authoritative, including labels which happen to
	// look generic. Gateway-generated placeholder titles are not useful in a
	// cross-device directory. The session key is transport identity, never UI
	// copy, so keep the title empty until the Gateway provides a real value.
	if label := strings.TrimSpace(stringFromAny(row["label"])); label != "" {
		return label
	}
	for _, field := range []string{"derivedTitle", "displayName", "title"} {
		if title := strings.TrimSpace(stringFromAny(row[field])); title != "" && !isOpenClawPlaceholderTitle(title) {
			return title
		}
	}
	return ""
}

func isOpenClawPlaceholderTitle(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "untitled", "unnamed", "new session", "未命名", "未命名会话", "新会话":
		return true
	default:
		return false
	}
}

func openClawAgentIDFromSessionKey(key string) string {
	parts := strings.SplitN(strings.TrimSpace(key), ":", 3)
	if len(parts) == 3 && strings.EqualFold(parts[0], "agent") {
		return strings.TrimSpace(parts[1])
	}
	return ""
}

func firstArray(payload map[string]any, keys ...string) []any {
	for _, key := range keys {
		switch value := payload[key].(type) {
		case []any:
			return value
		case map[string]any:
			if nested := firstArray(value, "sessions", "items", "data"); len(nested) > 0 {
				return nested
			}
		}
	}
	return nil
}

func boolFromAny(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func timeFromAny(value any) time.Time {
	if value == nil {
		return time.Time{}
	}
	if number, ok := numberFromAny(value); ok && number > 0 {
		if number < 1_000_000_000_000 {
			return time.Unix(int64(number), 0).UTC()
		}
		millis := int64(number)
		return time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond)).UTC()
	}
	text := strings.TrimSpace(stringFromAny(value))
	if text == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func numberFromAny(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		number, err := typed.Float64()
		return number, err == nil
	case string:
		number, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return number, err == nil
	default:
		return 0, false
	}
}

func isUnsupportedGatewayMethod(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "method") && (strings.Contains(text, "not found") || strings.Contains(text, "unknown") || strings.Contains(text, "unsupported"))
}

var sessionKeyPartRE = regexp.MustCompile(`[^a-zA-Z0-9._:-]+`)

func sanitizeSessionKeyPart(value string) string {
	value = strings.TrimSpace(value)
	value = sessionKeyPartRE.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return uuid.NewString()
	}
	if len(value) > 160 {
		value = value[:160]
	}
	return value
}

func stringFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if value, ok := m[key].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func joinDetails(items ...string) string {
	var out []string
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return strings.Join(out, "; ")
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func findMarkerEvidence(value any, marker string) (string, bool) {
	text := flattenVisibilityText(value)
	idx := strings.Index(text, marker)
	if idx < 0 {
		return "", false
	}
	prefix := text[:idx]
	suffix := text[idx+len(marker):]
	prefixRunes := []rune(prefix)
	if len(prefixRunes) > 80 {
		prefixRunes = prefixRunes[len(prefixRunes)-80:]
	}
	suffixRunes := []rune(suffix)
	if len(suffixRunes) > 80 {
		suffixRunes = suffixRunes[:80]
	}
	return strings.TrimSpace(string(prefixRunes) + marker + string(suffixRunes)), true
}

// Visibility verification also considers attachment names. Keep this separate
// from flattenText so an attachment-only turn does not fabricate body text.
func flattenVisibilityText(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		var parts []string
		for _, item := range v {
			if text := flattenVisibilityText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		var parts []string
		for _, key := range []string{"role", "text", "content", "message", "body", "fileName", "name"} {
			if text := flattenVisibilityText(v[key]); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return fmt.Sprint(v)
	}
}

func flattenText(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		var parts []string
		for _, item := range v {
			if text := flattenText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		var parts []string
		for _, key := range []string{"role", "text", "content", "message", "body"} {
			if text := flattenText(v[key]); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return fmt.Sprint(v)
	}
}

func latestAssistantText(value any) string {
	messages, ok := value.([]any)
	if !ok {
		return ""
	}
	for i := len(messages) - 1; i >= 0; i-- {
		msg, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(fmt.Sprint(msg["role"])))
		if role != "assistant" {
			continue
		}
		if text := flattenText(msg["content"]); strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func historyMessagesFromGatewayPayload(value any) []pluginbridge.HistoryMessage {
	messages, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]pluginbridge.HistoryMessage, 0, len(messages))
	for index, item := range messages {
		msg, ok := item.(map[string]any)
		if !ok {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(fmt.Sprint(msg["role"])))
		if role == "" {
			role = "assistant"
		}
		content := strings.TrimSpace(flattenText(firstNonNil(msg["content"], msg["message"], msg["text"], msg["body"])))
		createdAt := gatewayHistoryMessageTime(msg)
		if createdAt.IsZero() {
			createdAt = time.Unix(0, 0).UTC()
		}
		metadata := map[string]any{"raw_role": strings.TrimSpace(fmt.Sprint(msg["role"]))}
		if attachments := historyAttachmentDescriptors(msg); len(attachments) > 0 {
			metadata["attachments"] = attachments
		}
		out = append(out, pluginbridge.HistoryMessage{
			ID:        gatewayHistoryMessageID(msg, index),
			Role:      role,
			Type:      "text",
			Content:   content,
			Status:    gatewayHistoryMessageStatus(msg, role),
			CreatedAt: createdAt,
			UpdatedAt: firstNonZeroTime(timeFromAny(msg["updatedAt"]), createdAt),
			Metadata:  metadata,
		})
	}
	return out
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func historyAttachmentDescriptors(message map[string]any) []map[string]string {
	var out []map[string]string
	seen := map[string]bool{}
	var collect func(any)
	collect = func(value any) {
		switch typed := value.(type) {
		case []any:
			for _, item := range typed {
				collect(item)
			}
		case map[string]any:
			blockType := strings.ToLower(stringFromAny(typed["type"]))
			nested, _ := typed["attachment"].(map[string]any)
			name := firstNonEmpty(stringFromAny(typed["fileName"]), stringFromAny(typed["name"]), stringFromAny(typed["label"]))
			mimeType := firstNonEmpty(stringFromAny(typed["mimeType"]), stringFromAny(typed["mediaType"]))
			if nested != nil {
				name = firstNonEmpty(stringFromAny(nested["fileName"]), stringFromAny(nested["name"]), stringFromAny(nested["label"]), name)
				mimeType = firstNonEmpty(stringFromAny(nested["mimeType"]), stringFromAny(nested["mediaType"]), mimeType)
			}
			if blockType == "image" || blockType == "attachment" || blockType == "file" || nested != nil {
				kind := "document"
				if blockType == "image" || strings.HasPrefix(strings.ToLower(mimeType), "image/") {
					kind = "image"
				} else if strings.HasPrefix(strings.ToLower(mimeType), "audio/") {
					kind = "audio"
				}
				if name == "" {
					name = map[string]string{"image": "image", "audio": "audio", "document": "attachment"}[kind]
				}
				key := name + "\x00" + mimeType + "\x00" + kind
				if !seen[key] {
					seen[key] = true
					descriptor := map[string]string{"name": name, "kind": kind}
					if mimeType != "" {
						descriptor["mime_type"] = mimeType
					}
					out = append(out, descriptor)
				}
			}
			collect(typed["content"])
			collect(typed["attachments"])
		}
	}
	collect(message["content"])
	collect(message["attachments"])
	return out
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func limitOrDefault(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func normalizeWaitStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "ok", "success", "completed", "complete", "done":
		return "completed"
	case "failed", "failure", "error", "errored":
		return "failed"
	case "aborted", "cancelled", "canceled":
		return "failed"
	default:
		if strings.TrimSpace(status) == "" {
			return "completed"
		}
		return strings.ToLower(strings.TrimSpace(status))
	}
}

func trimRunes(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || value == "" {
		return value
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "..."
}

func sendPluginEvent(ctx context.Context, out chan<- pluginbridge.PluginEvent, event pluginbridge.PluginEvent) bool {
	select {
	case out <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

type pendingApprovalRecord struct {
	kind     string
	approval map[string]any
}

func pendingApprovalRecords(ctx context.Context, client rpcClient) []pendingApprovalRecord {
	var out []pendingApprovalRecord
	for _, kind := range []string{"exec", "plugin"} {
		payload, err := client.RequestAny(ctx, kind+".approval.list", map[string]any{})
		if err != nil {
			continue
		}
		items, _ := payload.([]any)
		for _, item := range items {
			approval, _ := item.(map[string]any)
			if approval != nil {
				out = append(out, pendingApprovalRecord{kind: kind, approval: approval})
			}
		}
	}
	return out
}

func (a *Adapter) approvalRequiredEvent(ctx context.Context, client rpcClient, endpoint, kind string, approval map[string]any) (pluginbridge.PluginEvent, bool) {
	sessionKey := approvalSessionKey(approval)
	view := approvalSnapshot(kind, approval)
	if sessionKey == "" || view == nil {
		return pluginbridge.PluginEvent{}, false
	}
	session := pluginbridge.NativeSession{
		PluginID:        a.ID(),
		NativeSessionID: sessionKey,
		Surface:         "gateway",
		Endpoint:        endpoint,
		Visible:         true,
	}
	run := map[string]any{
		"status":              "waiting_approval",
		"summary":             firstNonEmpty(stringFromAny(view["title"]), "OpenClaw 正在等待审批。"),
		"active":              true,
		"interruptible":       true,
		"approval":            view,
		"approval_blocked":    true,
		"approval_request_id": stringFromAny(view["id"]),
		"primary_action":      "interrupt",
	}
	payload := map[string]any{
		"method":              kind + ".approval.resolve",
		"approval_kind":       kind,
		"approval_request_id": stringFromAny(view["id"]),
		"title":               stringFromAny(view["title"]),
		"description":         stringFromAny(view["description"]),
		"command":             stringFromAny(view["command"]),
		"actions":             view["actions"],
	}
	event := pluginbridge.PluginEvent{
		ID:        stringFromAny(view["id"]),
		Type:      "approval.required",
		Status:    "waiting_approval",
		Summary:   firstNonEmpty(stringFromAny(view["title"]), stringFromAny(view["command"]), "OpenClaw 正在等待审批。"),
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}
	event = attachNativeSession(event, session)
	if snap, err := a.buildDetailSnapshotWithRun(ctx, client, session, run); err == nil {
		snap["approval"] = view
		event = withDetailSnapshot(event, snap)
	}
	return event, true
}

func approvalResolvedEvent(endpoint, kind string, payload map[string]any) (pluginbridge.PluginEvent, bool) {
	sessionKey := approvalSessionKey(payload)
	id := stringFromAny(payload["id"])
	decision := stringFromAny(payload["decision"])
	if sessionKey == "" || id == "" {
		return pluginbridge.PluginEvent{}, false
	}
	event := pluginbridge.PluginEvent{
		ID:      id + ":resolved",
		Type:    "approval.resolve",
		Status:  "completed",
		Summary: "OpenClaw 审批已处理。",
		Payload: map[string]any{
			"accepted":            !strings.EqualFold(decision, "deny"),
			"approval_request_id": id,
			"action_id":           "gateway:" + kind + ":" + decision,
			"message":             "OpenClaw 审批已处理。",
			"updated_at":          time.Now().UTC().UnixMilli(),
		},
		CreatedAt: time.Now().UTC(),
	}
	return attachNativeSession(event, pluginbridge.NativeSession{
		PluginID:        "openclaw",
		NativeSessionID: sessionKey,
		Surface:         "gateway",
		Endpoint:        endpoint,
		Visible:         true,
	}), true
}

func sessionIndexEvent(endpoint string, frame gatewayEventFrame) (pluginbridge.PluginEvent, bool) {
	row, _ := frame.Payload["session"].(map[string]any)
	if row == nil {
		row = frame.Payload
	}
	eventRow := make(map[string]any, len(row)+2)
	for key, value := range row {
		eventRow[key] = value
	}
	if stringFromAny(eventRow["key"]) == "" {
		eventRow["key"] = frame.Payload["sessionKey"]
	}
	reason := strings.ToLower(stringFromAny(frame.Payload["reason"]))
	if reason == "archive" || reason == "delete" {
		eventRow["archived"] = true
	}
	hintValue, validHint := openClawSessionHint(endpoint, eventRow)
	key := hintValue.NativeSessionID
	if key == "" {
		return pluginbridge.PluginEvent{
			ID:        fmt.Sprintf("openclaw-directory-%d", frame.Seq),
			Type:      "desktop.session.directory.reconciled",
			Status:    "completed",
			Summary:   "OpenClaw 会话目录已更新。",
			Payload:   map[string]any{"source": "openclaw-gateway"},
			CreatedAt: time.Now().UTC(),
		}, true
	}
	if !validHint {
		return pluginbridge.PluginEvent{}, false
	}
	updated := hintValue.LastActivityAt
	if updated.IsZero() {
		updated = timeFromAny(frame.Payload["ts"])
		if updated.IsZero() {
			updated = time.Now().UTC()
		}
		hintValue.LastActivityAt = updated
		hintValue.Metadata["sort_at"] = fmt.Sprint(updated.UnixMilli())
	}
	status := firstNonEmpty(hintValue.Metadata["status"], "idle")
	metadata := make(map[string]any, len(hintValue.Metadata))
	for metadataKey, value := range hintValue.Metadata {
		metadata[metadataKey] = value
	}
	hint := map[string]any{
		"plugin_id":             hintValue.PluginID,
		"native_session_id":     hintValue.NativeSessionID,
		"native_thread_id":      hintValue.NativeThreadID,
		"prism_conversation_id": hintValue.PrismConversationID,
		"surface":               hintValue.Surface,
		"endpoint":              hintValue.Endpoint,
		"cwd":                   hintValue.Cwd,
		"title":                 hintValue.Title,
		"last_activity_at":      updated.Format(time.RFC3339Nano),
		"metadata":              metadata,
	}
	if reason == "delete" {
		hint["change"] = "deleted"
	}
	session := pluginbridge.NativeSession{
		PluginID:        hintValue.PluginID,
		NativeSessionID: hintValue.NativeSessionID,
		NativeThreadID:  hintValue.NativeThreadID,
		Surface:         hintValue.Surface,
		Endpoint:        hintValue.Endpoint,
		Cwd:             hintValue.Cwd,
		Visible:         true,
	}
	event := pluginbridge.PluginEvent{
		ID:        fmt.Sprintf("openclaw-session-index-%s-%d", sanitizeSessionKeyPart(key), frame.Seq),
		Type:      "desktop.session.index.changed",
		Status:    status,
		Summary:   "OpenClaw 会话目录项已更新。",
		Payload:   map[string]any{"session_hint": hint},
		CreatedAt: time.Now().UTC(),
	}
	return attachNativeSession(event, session), true
}

func nativeSessionForGatewayEvent(endpoint string, frame gatewayEventFrame) pluginbridge.NativeSession {
	row, _ := frame.Payload["session"].(map[string]any)
	key := firstNonEmpty(stringFromAny(frame.Payload["sessionKey"]), stringFromAny(row["key"]))
	return pluginbridge.NativeSession{
		PluginID:        "openclaw",
		NativeSessionID: key,
		NativeThreadID:  firstNonEmpty(stringFromAny(frame.Payload["sessionId"]), stringFromAny(row["sessionId"])),
		Surface:         "gateway",
		Endpoint:        endpoint,
		Cwd:             firstNonEmpty(stringFromAny(row["cwd"]), stringFromAny(row["spawnedCwd"]), stringFromAny(row["execCwd"])),
		Visible:         true,
	}
}

func attachNativeSession(event pluginbridge.PluginEvent, session pluginbridge.NativeSession) pluginbridge.PluginEvent {
	if event.Payload == nil {
		event.Payload = map[string]any{}
	}
	event.Payload["native_session"] = map[string]any{
		"plugin_id":         session.PluginID,
		"native_session_id": session.NativeSessionID,
		"native_thread_id":  session.NativeThreadID,
		"surface":           session.Surface,
		"endpoint":          session.Endpoint,
		"cwd":               session.Cwd,
	}
	return event
}

func gatewayTimestamp(value any) string {
	timestamp := timeFromAny(value)
	if timestamp.IsZero() {
		return ""
	}
	return timestamp.Format(time.RFC3339Nano)
}

func convertGatewayEvent(sessionKey string, event gatewayEventFrame) (pluginbridge.PluginEvent, bool) {
	if strings.TrimSpace(sessionKey) != "" && stringFromAny(event.Payload["sessionKey"]) != "" && stringFromAny(event.Payload["sessionKey"]) != sessionKey {
		return pluginbridge.PluginEvent{}, false
	}
	switch event.Event {
	case "session.message":
		return convertSessionMessageEvent(event)
	case "agent":
		return convertAgentLifecycleEvent(event)
	default:
		return pluginbridge.PluginEvent{}, false
	}
}

func convertSessionMessageEvent(event gatewayEventFrame) (pluginbridge.PluginEvent, bool) {
	message, _ := event.Payload["message"].(map[string]any)
	status := strings.ToLower(strings.TrimSpace(stringFromAny(event.Payload["status"])))
	if status == "" {
		status = "running"
	}
	role := strings.ToLower(strings.TrimSpace(stringFromAny(message["role"])))
	summary := strings.TrimSpace(flattenText(message["content"]))
	if summary == "" {
		switch role {
		case "assistant":
			summary = "OpenClaw 正在生成回复。"
		case "user":
			summary = "OpenClaw 已记录用户消息。"
		default:
			summary = "OpenClaw 会话有新消息。"
		}
	}
	eventType := "message.updated"
	if role != "" {
		eventType = "message." + role
	}
	return pluginbridge.PluginEvent{
		ID:        firstNonEmpty(stringFromAny(event.Payload["messageId"]), fmt.Sprintf("session-message-%d", event.Seq)),
		Type:      eventType,
		Status:    status,
		Summary:   trimRunes(strings.Join(strings.Fields(summary), " "), 240),
		Payload:   event.Payload,
		CreatedAt: time.Now().UTC(),
	}, true
}

func convertAgentLifecycleEvent(event gatewayEventFrame) (pluginbridge.PluginEvent, bool) {
	data, _ := event.Payload["data"].(map[string]any)
	phase := strings.ToLower(strings.TrimSpace(stringFromAny(data["phase"])))
	if phase == "" {
		return pluginbridge.PluginEvent{}, false
	}
	status := "running"
	eventType := "run." + phase
	summary := "OpenClaw 正在处理。"
	switch phase {
	case "started", "running":
		status = "running"
		summary = "OpenClaw 已开始处理。"
	case "end", "ended", "completed", "complete", "success":
		status = "completed"
		eventType = "run.completed"
		summary = "OpenClaw 已完成。"
	case "failed", "error":
		status = "failed"
		eventType = "run.failed"
		summary = firstNonEmpty(stringFromAny(data["error"]), stringFromAny(data["livenessState"]), "OpenClaw 执行失败。")
	case "aborted", "cancelled", "canceled":
		status = "failed"
		eventType = "run.failed"
		summary = "OpenClaw 已中断。"
	}
	if detail := strings.TrimSpace(stringFromAny(data["fallbackStepFinalOutcome"])); detail != "" {
		summary = detail
	}
	return pluginbridge.PluginEvent{
		ID:        firstNonEmpty(stringFromAny(event.Payload["runId"]), fmt.Sprintf("agent-%d", event.Seq)),
		Type:      eventType,
		Status:    status,
		Summary:   trimRunes(strings.Join(strings.Fields(summary), " "), 240),
		Payload:   event.Payload,
		CreatedAt: time.Now().UTC(),
	}, true
}

func stringFromAny(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

package openclaw

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

// fakeRPCClient is a scriptable rpcClient for unit testing adapter logic
// without a real websocket gateway. It returns canned responses keyed by
// "<method>" (or "<method>|<param-value>" for finer matching) and records
// every call so tests can assert what was sent.
type fakeRPCClient struct {
	responses    map[string]map[string]any // key -> object payload
	anyResponses map[string]any            // key -> any payload, including arrays
	errors       map[string]error          // key -> error
	calls        []fakeCall
	events       chan gatewayEventFrame
}

type fakeCall struct {
	method string
	params map[string]any
}

func (f *fakeRPCClient) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	return f.RequestWithTimeout(ctx, method, params, time.Second)
}

func (f *fakeRPCClient) RequestWithTimeout(ctx context.Context, method string, params any, fallback time.Duration) (map[string]any, error) {
	p, _ := params.(map[string]any)
	f.calls = append(f.calls, fakeCall{method: method, params: p})
	// Try a param-keyed lookup first (e.g. "sessions.patch|model"), then bare method.
	if key, ok := paramKey(method, p); ok {
		if payload, hit := f.responses[key]; hit {
			return payload, nil
		}
		if err, hit := f.errors[key]; hit {
			return nil, err
		}
	}
	if payload, hit := f.responses[method]; hit {
		return payload, nil
	}
	if err, hit := f.errors[method]; hit {
		return nil, err
	}
	return map[string]any{}, nil
}

func (f *fakeRPCClient) RequestAny(ctx context.Context, method string, params any) (any, error) {
	return f.RequestAnyWithTimeout(ctx, method, params, time.Second)
}

func (f *fakeRPCClient) RequestAnyWithTimeout(ctx context.Context, method string, params any, fallback time.Duration) (any, error) {
	if payload, ok := f.anyResponses[method]; ok {
		p, _ := params.(map[string]any)
		f.calls = append(f.calls, fakeCall{method: method, params: p})
		return payload, nil
	}
	return f.RequestWithTimeout(ctx, method, params, fallback)
}

func (f *fakeRPCClient) NextEventWithTimeout(ctx context.Context, fallback time.Duration) (gatewayEventFrame, error) {
	if f.events == nil {
		<-ctx.Done()
		return gatewayEventFrame{}, ctx.Err()
	}
	select {
	case event := <-f.events:
		return event, nil
	case <-ctx.Done():
		return gatewayEventFrame{}, ctx.Err()
	}
}

func (f *fakeRPCClient) Close() error { return nil }

// paramKey builds a lookup key like "sessions.patch|model" when the params
// carry a discriminating value. Returns ok=false for methods we match by name.
func paramKey(method string, p map[string]any) (string, bool) {
	switch method {
	case "sessions.list":
		offset := 0
		if number, ok := numberFromAny(p["offset"]); ok {
			offset = int(number)
		}
		return method + "|offset:" + strconv.Itoa(offset), true
	case "sessions.patch":
		if v, ok := p["model"]; ok {
			return method + "|" + reflect.ValueOf(v).String(), true
		}
		if v, ok := p["thinkingLevel"]; ok {
			return method + "|thinkingLevel:" + reflect.ValueOf(v).String(), true
		}
		if v, ok := p["execSecurity"]; ok {
			return method + "|execSecurity:" + reflect.ValueOf(v).String(), true
		}
		if v, ok := p["label"]; ok {
			return method + "|label:" + reflect.ValueOf(v).String(), true
		}
		if v, ok := p["pinned"]; ok {
			return method + "|pinned:" + reflect.ValueOf(v).String(), true
		}
		if _, ok := p["archived"]; ok {
			return method + "|archived", true
		}
	}
	return "", false
}

// adapterWithFake returns an Adapter whose dial is replaced by a function
// returning the given fake client. Lets tests exercise buildDetailSnapshot,
// ControlSession and ResolveApproval without any network.
func adapterWithFake(fake *fakeRPCClient) *Adapter {
	a := New(Config{GatewayURL: "http://127.0.0.1:18789", GatewayToken: "test-token"})
	a.dial = func(ctx context.Context, cfg gatewayClientConfig) (rpcClient, error) {
		return fake, nil
	}
	return a
}

func sessionFixture() pluginbridge.NativeSession {
	return pluginbridge.NativeSession{
		PluginID:        "openclaw",
		NativeSessionID: "sess-123",
		NativeThreadID:  "sess-123",
		Surface:         "gateway",
	}
}

func TestBuildDetailSnapshotPopulatesModelAndOptions(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.describe": {
				"session": map[string]any{
					"model":         "claude-sonnet-4-6",
					"modelProvider": "anthropic",
					"thinkingLevel": "high",
					"thinkingLevels": []any{
						map[string]any{"id": "low", "label": "Low"},
						map[string]any{"id": "medium", "label": "Medium"},
						map[string]any{"id": "high", "label": "High"},
					},
					"execCwd":       "/tmp/proj",
					"derivedTitle":  "My Session",
					"pinned":        true,
					"totalTokens":   12345,
					"contextTokens": 200000,
				},
			},
			"models.list": {
				"models": []any{
					map[string]any{"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6", "provider": "anthropic", "reasoning": true},
					map[string]any{"id": "gpt-5.4", "name": "GPT-5.4", "provider": "openai"},
					map[string]any{"id": "disabled", "name": "Unavailable", "provider": "test", "available": false},
				},
			},
		},
	}
	a := adapterWithFake(fake)

	snap, err := a.buildDetailSnapshot(context.Background(), fake, sessionFixture())
	if err != nil {
		t.Fatalf("buildDetailSnapshot: %v", err)
	}

	// current_model carries the resolved model id.
	cm, _ := snap["current_model"].(map[string]any)
	if cm["key"] != "anthropic/claude-sonnet-4-6" {
		t.Fatalf("current_model.key = %v, want anthropic/claude-sonnet-4-6", cm["key"])
	}
	if cm["label"] != "claude-sonnet-4-6" {
		t.Fatalf("current_model.label = %v", cm["label"])
	}

	// model_options uses the `name` field (not label/displayName).
	opts, _ := snap["model_options"].([]any)
	if len(opts) != 2 {
		t.Fatalf("model_options len = %d, want 2", len(opts))
	}
	first := opts[0].(map[string]any)
	if first["key"] != "anthropic/claude-sonnet-4-6" {
		t.Fatalf("model_options[0].key = %v", first["key"])
	}
	if target := first["target"].(map[string]any); target["option_id"] != "anthropic/claude-sonnet-4-6" {
		t.Fatalf("model_options[0].target = %+v", target)
	}

	// reasoning_options come from the session's model-specific thinkingLevels.
	ropts, _ := snap["reasoning_options"].([]any)
	if len(ropts) != 3 || ropts[0].(map[string]any)["key"] != "low" {
		t.Fatalf("reasoning_options = %+v, want 3 levels starting with low", ropts)
	}
	if target := ropts[0].(map[string]any)["target"].(map[string]any); target["option_id"] != "low" {
		t.Fatalf("reasoning_options[0].target = %+v", target)
	}

	// cwd/title propagate from the session row; permission remains absent because
	// OpenClaw cannot reliably read a per-session permission value back.
	if snap["cwd"] != "/tmp/proj" {
		t.Fatalf("cwd = %v", snap["cwd"])
	}
	if snap["title"] != "My Session" {
		t.Fatalf("title = %v", snap["title"])
	}
	if _, exists := snap["current_permission"]; exists {
		t.Fatalf("current_permission must not be advertised: %+v", snap["current_permission"])
	}

	// actions list is present and switch-actions are unlocked in idle state.
	actions, _ := snap["actions"].([]any)
	if len(actions) == 0 {
		t.Fatal("expected non-empty actions")
	}
	if !actions[0].(map[string]any)["available"].(bool) {
		t.Fatal("model.switch should be available when idle")
	}
	pin := actions[len(actions)-1].(map[string]any)
	if pin["id"] != "pin" || pin["label"] != "取消置顶会话" || pin["target"].(map[string]any)["enabled"] != false {
		t.Fatalf("pin action must carry the idempotent next state: %+v", pin)
	}
}

func TestBuildDetailSnapshotDealsGracefullyWithGatewayErrors(t *testing.T) {
	// Every RPC errors: snapshot must still be returned (Hub sees empty fields)
	// rather than propagating the error, so a transient gateway hiccup doesn't
	// poison the whole detail stream.
	fake := &fakeRPCClient{
		errors: map[string]error{
			"sessions.describe": errors.New("boom"),
			"models.list":       errors.New("boom"),
		},
	}
	a := adapterWithFake(fake)

	snap, err := a.buildDetailSnapshot(context.Background(), fake, sessionFixture())
	if err != nil {
		t.Fatalf("buildDetailSnapshot should swallow per-RPC errors, got %v", err)
	}
	if snap["current_model"] != nil {
		t.Fatalf("current_model should be nil on error, got %v", snap["current_model"])
	}
	if ropts, _ := snap["reasoning_options"].([]any); len(ropts) != 0 {
		t.Fatalf("reasoning_options must not be fabricated, got %+v", ropts)
	}
}

func TestControlSessionSwitchModelCallsSessionsPatchAndReturnsRefreshedDetail(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.patch|anthropic/claude-sonnet-4-6": {"ok": true},
			"sessions.describe": {"session": map[string]any{
				"model": "claude-sonnet-4-6", "modelProvider": "anthropic",
			}},
			"models.list": {"models": []any{
				map[string]any{"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6", "provider": "anthropic"},
			}},
		},
	}
	a := adapterWithFake(fake)

	res, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
		Session: sessionFixture(),
		Action:  "model.switch",
		Target:  "anthropic/claude-sonnet-4-6",
	})
	if err != nil {
		t.Fatalf("ControlSession: %v", err)
	}
	if !res.OK {
		t.Fatal("expected OK=true")
	}
	if res.Action != "model.switch" {
		t.Fatalf("Action = %q", res.Action)
	}

	// Must have issued sessions.patch with the chosen model.
	var patchFound bool
	for _, c := range fake.calls {
		if c.method == "sessions.patch" && c.params["model"] == "anthropic/claude-sonnet-4-6" {
			patchFound = true
		}
	}
	if !patchFound {
		t.Fatalf("sessions.patch with model not issued; calls=%+v", fake.calls)
	}

	// Refreshed detail is attached so the Hub sees the new state synchronously.
	if res.Details == nil {
		t.Fatal("expected Details snapshot on control result")
	}
	if !res.DetailsConfirmed {
		t.Fatal("native RPC mutation must return DetailsConfirmed=true")
	}
}

func TestControlSessionAcceptsUnderscoreActionForm(t *testing.T) {
	// docs/06 §5.5: both "model.switch" and "model_switch" must be accepted.
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.patch|thinkingLevel:high": {"ok": true},
			"sessions.describe": {"session": map[string]any{
				"thinkingLevels": []any{map[string]any{"id": "high", "label": "High"}},
			}},
			"models.list": {"models": []any{}},
		},
	}
	a := adapterWithFake(fake)

	if _, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
		Session: sessionFixture(),
		Action:  "reasoning_switch", // underscore form
		Target:  "high",
	}); err != nil {
		t.Fatalf("reasoning_switch: %v", err)
	}

	var found bool
	for _, c := range fake.calls {
		if c.method == "sessions.patch" && c.params["thinkingLevel"] == "high" {
			found = true
		}
	}
	if !found {
		t.Fatal("underscore form reasoning_switch did not map to thinkingLevel patch")
	}
}

func TestControlSessionAcceptsStructuredOptionTarget(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"sessions.patch|thinkingLevel:minimal": {"ok": true},
		"sessions.describe": {"session": map[string]any{
			"model":          "claude-sonnet-4-6",
			"modelProvider":  "anthropic",
			"thinkingLevel":  "off",
			"thinkingLevels": []any{"off", "minimal", "low"},
		}},
		"models.list": {"models": []any{
			map[string]any{"id": "claude-sonnet-4-6", "provider": "anthropic", "reasoning": true},
		}},
	}}
	a := adapterWithFake(fake)

	if _, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
		Session: sessionFixture(),
		Action:  "reasoning.switch",
		Target:  map[string]any{"option_id": "minimal"},
	}); err != nil {
		t.Fatalf("reasoning.switch structured target: %v", err)
	}
	for _, call := range fake.calls {
		if call.method == "sessions.patch" && call.params["thinkingLevel"] == "minimal" {
			return
		}
	}
	t.Fatalf("sessions.patch with structured target not issued; calls=%+v", fake.calls)
}

func TestBuildDetailSnapshotHidesReasoningOptionsWhenCatalogDisablesReasoning(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"sessions.describe": {"session": map[string]any{
			"model":          "gpt-5.4",
			"modelProvider":  "free",
			"thinkingLevel":  "off",
			"thinkingLevels": []any{"off", "minimal", "low", "medium", "high"},
		}},
		"models.list": {"models": []any{
			map[string]any{"id": "gpt-5.4", "provider": "free", "reasoning": false, "available": true},
		}},
	}}
	a := adapterWithFake(fake)

	snap, err := a.buildDetailSnapshot(context.Background(), fake, sessionFixture())
	if err != nil {
		t.Fatalf("buildDetailSnapshot: %v", err)
	}
	if options := snap["reasoning_options"].([]any); len(options) != 0 {
		t.Fatalf("reasoning_options = %+v, want none for reasoning=false model", options)
	}
	if action := findAction(snap["actions"].([]any), "reasoning.switch"); action != nil {
		t.Fatalf("reasoning.switch must not be published: %+v", action)
	}
}

func TestControlSessionPinAcceptsIdempotentEnabledTarget(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.patch|pinned:false": {"ok": true},
			"sessions.describe":           {"session": map[string]any{"pinned": false}},
			"models.list":                 {"models": []any{}},
			"commands.list":               {"commands": []any{}},
		},
	}
	a := adapterWithFake(fake)
	if _, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
		Session: sessionFixture(),
		Action:  "pin",
		Target:  map[string]any{"enabled": false},
	}); err != nil {
		t.Fatalf("pin false: %v", err)
	}
	for _, call := range fake.calls {
		if call.method == "sessions.patch" && call.params["pinned"] == false {
			return
		}
	}
	t.Fatalf("pin target was not forwarded as false: %+v", fake.calls)
}

func TestControlSessionPinWithoutTargetTogglesNativeState(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"sessions.describe":           {"session": map[string]any{"pinned": true}},
		"sessions.patch|pinned:false": {"ok": true},
		"models.list":                 {"models": []any{}},
	}}
	a := adapterWithFake(fake)

	if _, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
		Session: sessionFixture(),
		Action:  "pin",
	}); err != nil {
		t.Fatalf("pin toggle: %v", err)
	}
	for _, call := range fake.calls {
		if call.method == "sessions.patch" && call.params["pinned"] == false {
			return
		}
	}
	t.Fatalf("sessions.patch did not toggle pinned state; calls=%+v", fake.calls)
}

func TestControlSessionRejectsUnsupportedAction(t *testing.T) {
	a := adapterWithFake(&fakeRPCClient{})
	_, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
		Session: sessionFixture(),
		Action:  "nonsense.action",
	})
	if err == nil {
		t.Fatal("expected error for unsupported action")
	}
	if !strings.Contains(err.Error(), "unsupported control action") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveApprovalDecodesActionID(t *testing.T) {
	// Approval snapshots carry the resolver kind in the action id.
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"exec.approval.resolve": {"applied": true},
		},
	}
	a := adapterWithFake(fake)

	if err := a.ResolveApproval(context.Background(), pluginbridge.ApprovalResolutionRequest{
		ApprovalRequestID: "appr-1",
		ActionID:          "gateway:exec:allow-once",
	}); err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}

	var resolveCall *fakeCall
	for i := range fake.calls {
		if fake.calls[i].method == "exec.approval.resolve" {
			resolveCall = &fake.calls[i]
			break
		}
	}
	if resolveCall == nil {
		t.Fatal("exec.approval.resolve not called")
	}
	if resolveCall.params["decision"] != "allow-once" {
		t.Fatalf("decision = %v, want allow-once", resolveCall.params["decision"])
	}
	if _, exists := resolveCall.params["kind"]; exists {
		t.Fatalf("resolver method already encodes kind; params must not contain it: %+v", resolveCall.params)
	}
	if resolveCall.params["id"] != "appr-1" {
		t.Fatalf("id = %v, want appr-1", resolveCall.params["id"])
	}
}

func TestResolvePluginApprovalUsesPluginResolver(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"plugin.approval.resolve": {"applied": true},
	}}
	a := adapterWithFake(fake)
	if err := a.ResolveApproval(context.Background(), pluginbridge.ApprovalResolutionRequest{
		ApprovalRequestID: "plugin-approval-1",
		ActionID:          "gateway:plugin:deny",
	}); err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0].method != "plugin.approval.resolve" {
		t.Fatalf("unexpected calls: %+v", fake.calls)
	}
}

func TestResolveApprovalRejectsBadActionID(t *testing.T) {
	a := adapterWithFake(&fakeRPCClient{})
	err := a.ResolveApproval(context.Background(), pluginbridge.ApprovalResolutionRequest{
		ApprovalRequestID: "appr-1",
		ActionID:          "garbage",
	})
	if err == nil {
		t.Fatal("expected error for undecodable action id")
	}
}

// TestCapabilityDeclarationConsistency enforces docs/06 §5.3: a capability
// flag declared true implies the corresponding interface is implemented.
func TestCapabilityDeclarationConsistency(t *testing.T) {
	a := New(Config{GatewayURL: "http://127.0.0.1:18789"})
	if _, ok := interface{}(a).(pluginbridge.SessionController); !ok {
		t.Fatal("CanControlSession=true declared but *Adapter does not implement SessionController")
	}
	if _, ok := interface{}(a).(pluginbridge.ApprovalResolver); !ok {
		t.Fatal("CanApproval=true declared but *Adapter does not implement ApprovalResolver")
	}
	if _, ok := interface{}(a).(pluginbridge.PluginWideSubscriber); !ok {
		t.Fatal("CanPluginWideWatch=true declared but *Adapter does not implement PluginWideSubscriber")
	}
	if _, ok := interface{}(a).(pluginbridge.StatusReader); !ok {
		t.Fatal("CanReadStatus=true declared but *Adapter does not implement StatusReader")
	}
}

func TestContextWindowFilledFromSessionDescription(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.describe": {"session": map[string]any{"totalTokens": 12345, "contextTokens": 200000}},
			"models.list":       {"models": []any{}},
		},
	}
	a := adapterWithFake(fake)
	snap, err := a.buildDetailSnapshot(context.Background(), fake, sessionFixture())
	if err != nil {
		t.Fatalf("buildDetailSnapshot: %v", err)
	}
	if snap["context_tokens_used"] != int64(12345) {
		t.Fatalf("context_tokens_used = %v, want 12345", snap["context_tokens_used"])
	}
	if snap["context_window_total"] != int64(200000) {
		t.Fatalf("context_window_total = %v, want 200000", snap["context_window_total"])
	}
	percent, _ := snap["context_window_usage_percent"].(float64)
	if percent < 6 || percent >= 7 {
		t.Fatalf("context_window_usage_percent = %v, want [6,7)", snap["context_window_usage_percent"])
	}
	if snap["context_window"] != "上下文窗口 200000" {
		t.Fatalf("context_window = %v", snap["context_window"])
	}
	// The Hermes-only guessed RPC must never be called.
	for _, c := range fake.calls {
		if c.method == "session.context_breakdown" {
			t.Fatal("session.context_breakdown must not be called")
		}
	}
}

func TestRunOverrideFromAgentEventDrivesControlsLocking(t *testing.T) {
	// An agent "running" event must produce a run with status=running and lock
	// the model/reasoning/permission switches in the actions list.
	data := map[string]any{"phase": "running"}
	run := runFromAgentEvent(data)
	if run["status"] != "running" {
		t.Fatalf("status = %v, want running", run["status"])
	}
	if run["interruptible"] != true {
		t.Fatal("running run must be interruptible")
	}
	if run["primary_action"] != "interrupt" {
		t.Fatalf("primary_action = %v, want interrupt", run["primary_action"])
	}

	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.describe": {"session": map[string]any{
				"thinkingLevels": []any{map[string]any{"id": "high", "label": "High"}},
			}},
			"models.list": {"models": []any{
				map[string]any{"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6", "provider": "anthropic"},
			}},
		},
	}
	a := adapterWithFake(fake)
	snap, err := a.buildDetailSnapshotWithRun(context.Background(), fake, sessionFixture(), run)
	if err != nil {
		t.Fatalf("buildDetailSnapshotWithRun: %v", err)
	}
	// Top-level fields mirror the run.
	if snap["status"] != "running" {
		t.Fatalf("snap.status = %v, want running", snap["status"])
	}
	if snap["primary_action"] != "interrupt" {
		t.Fatalf("snap.primary_action = %v, want interrupt", snap["primary_action"])
	}
	// Controls locked: model.switch unavailable.
	actions := snap["actions"].([]any)
	modelSwitch := findAction(actions, "model.switch")
	if modelSwitch == nil {
		t.Fatal("expected model.switch action")
	}
	if modelSwitch["available"] != false {
		t.Fatal("model.switch must be unavailable while running")
	}
}

func findAction(actions []any, id string) map[string]any {
	for _, raw := range actions {
		action, _ := raw.(map[string]any)
		if action["id"] == id {
			return action
		}
	}
	return nil
}

func TestRunTerminalStateFromWaitStatus(t *testing.T) {
	run := runFromWaitStatus(map[string]any{
		"status": "completed",
		"error":  "",
	})
	if run["status"] != "completed" {
		t.Fatalf("status = %v, want completed", run["status"])
	}
	if run["primary_action"] != "send" {
		t.Fatalf("terminal primary_action = %v, want send", run["primary_action"])
	}

	runFail := runFromWaitStatus(map[string]any{"status": "failed", "error": "boom"})
	if runFail["status"] != "failed" {
		t.Fatalf("status = %v, want failed", runFail["status"])
	}
}

func TestControlSessionUsesNativeCompactAndDeleteRPCs(t *testing.T) {
	for _, test := range []struct {
		action string
		method string
	}{
		{action: "context.compact", method: "sessions.compact"},
		{action: "delete", method: "sessions.delete"},
	} {
		t.Run(test.action, func(t *testing.T) {
			fake := &fakeRPCClient{responses: map[string]map[string]any{
				test.method:         {"ok": true},
				"sessions.describe": {"session": map[string]any{}},
				"models.list":       {"models": []any{}},
			}}
			a := adapterWithFake(fake)
			result, err := a.ControlSession(context.Background(), pluginbridge.ControlSessionRequest{
				Session: sessionFixture(), Action: test.action,
			})
			if err != nil {
				t.Fatalf("ControlSession: %v", err)
			}
			if !result.DetailsConfirmed {
				t.Fatal("native mutation must be confirmed")
			}
			for _, call := range fake.calls {
				if call.method == test.method {
					return
				}
			}
			t.Fatalf("%s not called: %+v", test.method, fake.calls)
		})
	}
}

func TestReadStatusTreatsZeroWaitTimeoutAsActiveRun(t *testing.T) {
	fake := &fakeRPCClient{responses: map[string]map[string]any{
		"agent.wait": {"status": "timeout", "timeoutPhase": "gateway_draining"},
		"sessions.describe": {"session": map[string]any{
			"model": "gpt-5.4", "modelProvider": "openai", "thinkingLevel": "high",
		}},
	}}
	a := adapterWithFake(fake)
	status, err := a.ReadStatus(context.Background(), sessionFixture(), "run-1")
	if err != nil {
		t.Fatalf("ReadStatus: %v", err)
	}
	if status.Status != "running" || !status.Interruptible || status.PrimaryAction != "interrupt" {
		t.Fatalf("unexpected active status: %+v", status)
	}
	if model, _ := status.Model.(map[string]any); model["key"] != "openai/gpt-5.4" {
		t.Fatalf("unexpected model: %+v", status.Model)
	}
}

func TestSubscribePluginRoutesDirectoryMessageAndApprovalEvents(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.subscribe": {"subscribed": true},
			"sessions.describe":  {"session": map[string]any{}},
			"models.list":        {"models": []any{}},
		},
		anyResponses: map[string]any{
			"exec.approval.list":   []any{},
			"plugin.approval.list": []any{},
		},
		events: make(chan gatewayEventFrame, 4),
	}
	a := adapterWithFake(fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := a.SubscribePlugin(ctx)
	if err != nil {
		t.Fatalf("SubscribePlugin: %v", err)
	}
	initial := receivePluginEvent(t, events)
	if initial.Type != "desktop.session.directory.reconciled" {
		t.Fatalf("initial event: %+v", initial)
	}

	fake.events <- gatewayEventFrame{Type: "event", Event: "sessions.changed", Seq: 1, Payload: map[string]any{
		"sessionKey": "agent:main:prism:one",
		"session": map[string]any{
			"key": "agent:main:prism:one", "sessionId": "thread-1", "label": "One", "kind": "direct",
			"origin": map[string]any{"provider": "wechat"}, "updatedAt": time.Now().UnixMilli(),
		},
	}}
	index := receivePluginEvent(t, events)
	if index.Type != "desktop.session.index.changed" || nativeSessionID(index) != "agent:main:prism:one" {
		t.Fatalf("index event: %+v", index)
	}
	hint, _ := index.Payload["session_hint"].(map[string]any)
	metadata, _ := hint["metadata"].(map[string]any)
	if metadata["session_kind"] != "direct" || metadata["origin_provider"] != "wechat" {
		t.Fatalf("incremental index lost session classification: %+v", hint)
	}

	fake.events <- gatewayEventFrame{Type: "event", Event: "session.message", Seq: 2, Payload: map[string]any{
		"sessionKey": "agent:main:prism:one",
		"messageId":  "message-1",
		"message":    map[string]any{"role": "assistant", "content": "hello"},
	}}
	message := receivePluginEvent(t, events)
	if message.Type != "message.assistant" || nativeSessionID(message) != "agent:main:prism:one" {
		t.Fatalf("message event: %+v", message)
	}

	fake.events <- gatewayEventFrame{Type: "event", Event: "exec.approval.requested", Seq: 3, Payload: map[string]any{
		"id": "approval-1",
		"request": map[string]any{
			"sessionKey":       "agent:main:prism:one",
			"command":          "sleep 1",
			"allowedDecisions": []any{"allow-once", "deny"},
		},
	}}
	approval := receivePluginEvent(t, events)
	if approval.Type != "approval.required" || approval.Status != "waiting_approval" || nativeSessionID(approval) != "agent:main:prism:one" {
		t.Fatalf("approval event: %+v", approval)
	}
	actions, _ := approval.Payload["actions"].([]any)
	if len(actions) != 2 || actions[0].(map[string]any)["id"] != "gateway:exec:allow-once" {
		t.Fatalf("approval actions: %+v", actions)
	}
}

func TestSubscribePluginReplaysPendingApprovalAfterReconnect(t *testing.T) {
	fake := &fakeRPCClient{
		responses: map[string]map[string]any{
			"sessions.subscribe": {"subscribed": true},
			"sessions.describe":  {"session": map[string]any{}},
			"models.list":        {"models": []any{}},
		},
		anyResponses: map[string]any{
			"exec.approval.list": []any{map[string]any{
				"id": "pending-1",
				"request": map[string]any{
					"sessionKey":       "agent:main:prism:one",
					"command":          "sleep 1",
					"allowedDecisions": []any{"allow-once", "deny"},
				},
			}},
			"plugin.approval.list": []any{},
		},
		events: make(chan gatewayEventFrame),
	}
	a := adapterWithFake(fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := a.SubscribePlugin(ctx)
	if err != nil {
		t.Fatalf("SubscribePlugin: %v", err)
	}
	_ = receivePluginEvent(t, events) // initial directory reconciliation
	replayed := receivePluginEvent(t, events)
	if replayed.Type != "approval.required" || replayed.ID != "pending-1" || nativeSessionID(replayed) != "agent:main:prism:one" {
		t.Fatalf("pending approval was not replayed: %+v", replayed)
	}
}

func receivePluginEvent(t *testing.T, events <-chan pluginbridge.PluginEvent) pluginbridge.PluginEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for plugin event")
		return pluginbridge.PluginEvent{}
	}
}

func nativeSessionID(event pluginbridge.PluginEvent) string {
	native, _ := event.Payload["native_session"].(map[string]any)
	return stringFromAny(native["native_session_id"])
}

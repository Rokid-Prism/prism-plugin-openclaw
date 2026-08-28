package openclaw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

var _ pluginbridge.DetailReader = (*Adapter)(nil)

// This file implements the read side of the unified capability contract
// (docs/06 §5.6): building the detail_snapshot that the Hub reads from any
// PluginEvent.Payload["detail_snapshot"]. The writable side (ControlSession)
// lives in control.go.
//
// Field names below match internal/bridge/detail_snapshot.go's
// canonicalDetailSnapshot, which is the authoritative Hub-side consumer.
// Where a field is unavailable from the gateway we omit it (Hub treats absent
// keys as "unsupported" / null), per docs/05 §5 and docs/06 §5.6.

// buildDetailSnapshot assembles the unified detail_snapshot for a session by
// querying the gateway. It is the single read-path used by Subscribe, WaitForRun
// and ControlSession so the three stay in sync.
//
// client must already be connected; caller owns Close().
// runOverride, when non-nil, supplies the live run state (status/phase/preview/
// approval/...) so the snapshot reflects what the caller already knows from an
// agent event or a terminal wait; when nil an idle placeholder is seeded.
func (a *Adapter) buildDetailSnapshot(ctx context.Context, client rpcClient, session pluginbridge.NativeSession) (map[string]any, error) {
	return a.buildDetailSnapshotWithRun(ctx, client, session, nil)
}

// ReadDetail exposes the same authoritative native snapshot used by runtime
// events and control responses, without changing the selected session.
func (a *Adapter) ReadDetail(ctx context.Context, session pluginbridge.NativeSession) (map[string]any, error) {
	client, err := a.dialClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return a.buildDetailSnapshot(ctx, client, session)
}

func (a *Adapter) buildDetailSnapshotWithRun(ctx context.Context, client rpcClient, session pluginbridge.NativeSession, runOverride map[string]any) (map[string]any, error) {
	sessionKey := strings.TrimSpace(session.NativeSessionID)
	if sessionKey == "" {
		return nil, fmt.Errorf("openclaw: session key required for detail snapshot")
	}

	snap := map[string]any{
		"plugin_id":          a.ID(),
		"conversation_id":    sessionKey,
		"desktop_foreground": false,
		"detail_stale":       false,
		"updated_at":         time.Now().UTC().UnixMilli(),
		"primary_action":     "send",
		"model_options":      []any{},
		"reasoning_options":  []any{},
		"actions":            []any{},
	}

	// 1) Session row: model, thinkingLevel, execSecurity/execAsk, cwd, title.
	row, err := a.sessionDescribe(ctx, client, sessionKey)
	if err == nil && row != nil {
		applySessionRow(snap, row)
	}

	// 2) Model catalog -> model_options (field is `name` on each entry).
	if models, err := a.modelsList(ctx, client, ""); err == nil {
		snap["model_options"] = modelsToOptions(models)
		if row != nil {
			snap["reasoning_options"] = reasoningOptionsForModel(row, models)
		}
	} else if row != nil {
		// The session row is still useful when the model catalog is temporarily
		// unavailable. When both exist, the catalog's explicit reasoning=false
		// is authoritative because sessions.patch enforces it.
		snap["reasoning_options"] = reasoningOptionsFromRow(row)
	}

	// 3) Run state. Use the caller's live override when available (agent event /
	// terminal wait); otherwise seed an idle placeholder. Subscribe overrides on
	// each agent event, WaitForRun on the terminal snapshot.
	run := runOverride
	if run == nil {
		run = idleRun()
	}
	if kind, approval := firstPendingApprovalForSession(ctx, client, sessionKey); approval != nil {
		approvalView := approvalSnapshot(kind, approval)
		run = map[string]any{
			"status":              "waiting_approval",
			"summary":             firstNonEmpty(stringFromAny(approvalView["title"]), "OpenClaw 正在等待审批。"),
			"active":              true,
			"interruptible":       true,
			"approval":            approvalView,
			"approval_blocked":    true,
			"approval_request_id": stringFromAny(approvalView["id"]),
			"primary_action":      "interrupt",
		}
		snap["approval"] = approvalView
	}
	snap["run"] = run
	// Mirror run primary_action/status onto the top-level fields the mobile
	// client reads directly, and re-derive controlsLocked from the real status.
	snap["primary_action"] = firstNonEmpty(stringFromAny(run["primary_action"]), "send")
	if st := stringFromAny(run["status"]); st != "" {
		snap["status"] = st
	}

	// 4) Actions only advertise controls backed by a real Gateway RPC and a
	// verified option catalog. OpenClaw does not expose a stable per-session
	// permission value, so permission.switch is intentionally absent.
	controlsLocked := isControlsLocked(snap)
	modelOptions, _ := snap["model_options"].([]any)
	reasoningOptions, _ := snap["reasoning_options"].([]any)
	snap["actions"] = buildDetailActions(controlsLocked, boolFromAny(snap["pinned"]), len(modelOptions) > 0, len(reasoningOptions) > 0)

	return snap, nil
}

// idleRun is the placeholder run object for a session with no known activity.
func idleRun() map[string]any {
	return map[string]any{
		"status":           "idle",
		"summary":          "当前会话空闲。",
		"active":           false,
		"interruptible":    false,
		"approval":         nil,
		"approval_blocked": false,
		"primary_action":   "send",
	}
}

// runFromAgentEvent builds a run object from a live gateway agent lifecycle
// event payload (the `data` sub-object). Used by Subscribe so each agent event
// refreshes the run state inside the pushed detail_snapshot.
func runFromAgentEvent(data map[string]any) map[string]any {
	phase := strings.ToLower(strings.TrimSpace(stringFromAny(data["phase"])))
	status := "idle"
	interruptible := false
	summary := "当前会话空闲。"
	primaryAction := "send"
	switch phase {
	case "started", "running":
		status = "running"
		interruptible = true
		summary = "OpenClaw 正在回复…"
		primaryAction = "interrupt"
	case "ended", "completed", "complete", "success":
		status = "completed"
		summary = "OpenClaw 已完成。"
	case "failed", "error":
		status = "failed"
		summary = firstNonEmpty(stringFromAny(data["error"]), stringFromAny(data["livenessState"]), "OpenClaw 执行失败。")
	case "aborted", "cancelled", "canceled":
		status = "failed"
		summary = "OpenClaw 已中断。"
	}
	if detail := strings.TrimSpace(stringFromAny(data["fallbackStepFinalOutcome"])); detail != "" {
		summary = detail
	}
	preview := strings.TrimSpace(stringFromAny(data["preview"]))
	run := map[string]any{
		"status":           status,
		"summary":          trimRunes(strings.Join(strings.Fields(summary), " "), 240),
		"active":           status == "running" || status == "waiting_approval",
		"interruptible":    interruptible,
		"approval":         nil,
		"approval_blocked": false,
		"primary_action":   primaryAction,
	}
	// Phase object mirrors codex's run.phase {id,label,detail,source}.
	if phase != "" && phase != "idle" {
		run["phase"] = map[string]any{
			"id":     phase,
			"label":  summary,
			"source": "agent",
		}
	}
	if preview != "" {
		run["preview"] = preview
	}
	return run
}

// runFromWaitStatus builds a run object from an agent.wait terminal snapshot.
func runFromWaitStatus(payload map[string]any) map[string]any {
	status := normalizeWaitStatus(stringFromMap(payload, "status"))
	summary := strings.TrimSpace(stringFromMap(payload, "error"))
	interruptible := false
	switch status {
	case "completed":
		if summary == "" {
			summary = "OpenClaw 已完成。"
		}
	case "failed":
		if summary == "" {
			summary = "OpenClaw 执行失败。"
		}
	default:
		if summary == "" {
			summary = "OpenClaw 状态：" + status
		}
	}
	return map[string]any{
		"status":           status,
		"summary":          trimRunes(strings.Join(strings.Fields(summary), " "), 240),
		"active":           false,
		"interruptible":    interruptible,
		"approval":         nil,
		"approval_blocked": false,
		"primary_action":   "send",
	}
}

// sessionDescribe calls gateway sessions.describe and returns the session row.
func (a *Adapter) sessionDescribe(ctx context.Context, client rpcClient, sessionKey string) (map[string]any, error) {
	payload, err := client.Request(ctx, "sessions.describe", map[string]any{
		"key":                  sessionKey,
		"includeDerivedTitles": true,
	})
	if err != nil {
		return nil, err
	}
	row, _ := payload["session"].(map[string]any)
	return row, nil
}

// modelsList calls gateway models.list. view="" lets the server pick its default.
func (a *Adapter) modelsList(ctx context.Context, client rpcClient, view string) ([]map[string]any, error) {
	params := map[string]any{}
	if v := strings.TrimSpace(view); v != "" {
		params["view"] = v
	}
	payload, err := client.Request(ctx, "models.list", params)
	if err != nil {
		return nil, err
	}
	raw, _ := payload["models"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, m := range raw {
		if mm, ok := m.(map[string]any); ok {
			out = append(out, mm)
		}
	}
	return out, nil
}

// --- helpers ---

func applySessionRow(snap map[string]any, row map[string]any) {
	if v := stringFromAny(row["model"]); v != "" {
		provider := stringFromAny(row["modelProvider"])
		key := v
		if provider != "" && !strings.Contains(v, "/") {
			key = provider + "/" + v
		}
		snap["current_model"] = optionObjectWithLabel(key, v)
	}
	if v := firstNonEmpty(stringFromAny(row["thinkingLevel"]), stringFromAny(row["thinkingDefault"])); v != "" {
		snap["current_reasoning"] = optionObject(v, "")
	}
	if v := stringFromAny(row["execCwd"]); v != "" {
		snap["cwd"] = v
	} else if v := stringFromAny(row["spawnedCwd"]); v != "" {
		snap["cwd"] = v
	}
	if v := stringFromAny(row["derivedTitle"]); v != "" {
		snap["title"] = v
	} else if v := stringFromAny(row["displayName"]); v != "" {
		snap["title"] = v
	} else if v := stringFromAny(row["label"]); v != "" {
		snap["title"] = v
	}
	if _, ok := row["pinned"]; ok {
		snap["pinned"] = boolFromAny(row["pinned"])
	}
	applyContextFromRow(snap, row)
}

// optionObject builds the {key,label,displayName,value} shape that codex uses
// for current_model / current_reasoning / current_permission. Keeping the same
// shape lets the Mobile client render OpenClaw exactly like Codex.
func optionObject(key, provider string) map[string]any {
	label := key
	if provider != "" {
		label = fmt.Sprintf("%s (%s)", key, provider)
	}
	return map[string]any{
		"key":         key,
		"value":       key,
		"label":       label,
		"displayName": key,
	}
}

func optionObjectWithLabel(key, label string) map[string]any {
	key = strings.TrimSpace(key)
	label = strings.TrimSpace(label)
	if label == "" {
		label = key
	}
	return map[string]any{
		"key":         key,
		"value":       key,
		"label":       label,
		"displayName": label,
	}
}

func optionObjectWithTarget(key, label string) map[string]any {
	option := optionObjectWithLabel(key, label)
	option["target"] = map[string]any{"option_id": key}
	return option
}

func modelsToOptions(models []map[string]any) []any {
	out := make([]any, 0, len(models))
	seen := map[string]bool{}
	for _, m := range models {
		if available, exists := m["available"]; exists && !boolFromAny(available) {
			continue
		}
		id := strings.TrimSpace(stringFromAny(m["id"]))
		provider := strings.TrimSpace(stringFromAny(m["provider"]))
		if id == "" {
			continue
		}
		key := id
		if provider != "" && !strings.Contains(id, "/") {
			key = provider + "/" + id
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		label := firstNonEmpty(stringFromAny(m["name"]), key)
		out = append(out, optionObjectWithTarget(key, label))
	}
	return out
}

func reasoningOptionsFromRow(row map[string]any) []any {
	levels, _ := row["thinkingLevels"].([]any)
	out := make([]any, 0, len(levels))
	seen := map[string]bool{}
	for _, value := range levels {
		id := ""
		label := ""
		switch level := value.(type) {
		case string:
			id = strings.TrimSpace(level)
		case map[string]any:
			id = firstNonEmpty(stringFromAny(level["id"]), stringFromAny(level["key"]), stringFromAny(level["value"]))
			label = stringFromAny(level["label"])
		}
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, optionObjectWithTarget(id, label))
	}
	return out
}

func reasoningOptionsForModel(row map[string]any, models []map[string]any) []any {
	modelID := strings.TrimSpace(stringFromAny(row["model"]))
	provider := strings.TrimSpace(stringFromAny(row["modelProvider"]))
	for _, model := range models {
		if !strings.EqualFold(strings.TrimSpace(stringFromAny(model["id"])), modelID) ||
			!strings.EqualFold(strings.TrimSpace(stringFromAny(model["provider"])), provider) {
			continue
		}
		if supported, exists := model["reasoning"]; exists && !boolFromAny(supported) {
			return []any{}
		}
		break
	}
	return reasoningOptionsFromRow(row)
}

func applyContextFromRow(snap, row map[string]any) {
	used, usedOK := numberFromAny(row["totalTokens"])
	total, totalOK := numberFromAny(row["contextTokens"])
	if usedOK && used >= 0 {
		snap["context_tokens_used"] = int64(used)
	}
	if totalOK && total > 0 {
		snap["context_window_total"] = int64(total)
		snap["context_window"] = fmt.Sprintf("上下文窗口 %d", int64(total))
	}
	if usedOK && totalOK && total > 0 {
		percent := used / total * 100
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
		snap["context_window_usage_percent"] = percent
	}
}

func buildDetailActions(controlsLocked, pinned, canSwitchModel, canSwitchReasoning bool) []any {
	makeAction := func(id, label string, available bool) map[string]any {
		return map[string]any{"id": id, "label": label, "available": available}
	}
	actions := make([]any, 0, 8)
	if canSwitchModel {
		actions = append(actions, makeAction("model.switch", "切换模型", !controlsLocked))
	}
	if canSwitchReasoning {
		actions = append(actions, makeAction("reasoning.switch", "切换推理", !controlsLocked))
	}
	// These are always available regardless of run state.
	for _, a := range []struct{ id, label string }{
		{"context.compact", "压缩上下文"},
		{"rename", "重命名会话"},
		{"archive", "归档会话"},
		{"delete", "删除会话"},
	} {
		actions = append(actions, makeAction(a.id, a.label, true))
	}
	actions = append(actions, map[string]any{
		"id": "pin", "label": map[bool]string{true: "取消置顶会话", false: "置顶会话"}[pinned],
		"available": true, "target": map[string]any{"enabled": !pinned},
	})
	return actions
}

// isControlsLocked returns true when the run status would block model/reasoning
// switches. We don't have a live run here, so we treat the placeholder as idle
// (not locked). The Subscribe loop overrides this with the real run status.
func isControlsLocked(snap map[string]any) bool {
	run, _ := snap["run"].(map[string]any)
	if run == nil {
		return false
	}
	switch strings.ToLower(stringFromAny(run["status"])) {
	case "running", "waiting", "waiting_approval":
		return true
	}
	return false
}

// messagesSignatureFromHistory computes a sha256 over message IDs/roles/content
// so the Hub knows when to refetch body text. Mirrors codex's signature intent
// (docs/05 §4.2): covers user-visible order/ID/role/content, no binary.
func messagesSignatureFromHistory(messages []map[string]any) string {
	keys := make([]string, 0, len(messages))
	for _, m := range messages {
		id := stringFromAny(m["id"])
		if id == "" {
			id = stringFromAny(m["messageId"])
		}
		role := strings.TrimSpace(stringFromAny(m["role"]))
		content := flattenText(m["content"])
		keys = append(keys, fmt.Sprintf("%s|%s|%s", id, role, content))
	}
	h := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(h[:])
}

func approvalSnapshot(kind string, approval map[string]any) map[string]any {
	if approval == nil {
		return nil
	}
	request, _ := approval["request"].(map[string]any)
	if request == nil {
		request = approval
	}
	id := stringFromAny(approval["id"])
	if id == "" {
		return nil
	}
	out := map[string]any{
		"id":     id,
		"status": "pending",
		"source": "gateway",
		"kind":   kind,
	}
	out["title"] = firstNonEmpty(stringFromAny(request["title"]), map[string]string{"exec": "OpenClaw 命令审批", "plugin": "OpenClaw 插件审批"}[kind])
	out["command"] = firstNonEmpty(stringFromAny(request["command"]), stringFromAny(request["commandText"]))
	out["description"] = firstNonEmpty(stringFromAny(request["description"]), stringFromAny(request["warningText"]))
	if decisions, ok := request["allowedDecisions"].([]any); ok {
		actions := make([]any, 0, len(decisions))
		for _, d := range decisions {
			decision := stringFromAny(d)
			if !isValidDecision(decision) {
				continue
			}
			actions = append(actions, map[string]any{
				"id":             fmt.Sprintf("gateway:%s:%s", kind, decision),
				"label":          decision,
				"style":          approvalActionStyle(decision),
				"requires_input": false,
				"available":      true,
			})
		}
		out["actions"] = actions
	}
	return out
}

func approvalActionStyle(decision string) string {
	switch strings.ToLower(decision) {
	case "allow-once", "allow-always":
		return "primary"
	case "deny":
		return "danger"
	default:
		return "secondary"
	}
}

func firstPendingApprovalForSession(ctx context.Context, client rpcClient, sessionKey string) (string, map[string]any) {
	for _, kind := range []string{"exec", "plugin"} {
		payload, err := client.RequestAny(ctx, kind+".approval.list", map[string]any{})
		if err != nil {
			continue
		}
		items, _ := payload.([]any)
		for _, item := range items {
			approval, _ := item.(map[string]any)
			if approvalSessionKey(approval) == sessionKey {
				return kind, approval
			}
		}
	}
	return "", nil
}

func approvalSessionKey(approval map[string]any) string {
	request, _ := approval["request"].(map[string]any)
	return firstNonEmpty(stringFromAny(request["sessionKey"]), stringFromAny(approval["sessionKey"]))
}

// withDetailSnapshot attaches a built snapshot to a PluginEvent.Payload so the
// Hub picks it up via detailSnapshotPayloadFromPluginEvent. If build fails the
// event is returned unchanged (the Hub still sees the raw text Summary). Fields
// the caller already set on the snapshot (e.g. live run/approval) are preserved.
func withDetailSnapshot(event pluginbridge.PluginEvent, snap map[string]any) pluginbridge.PluginEvent {
	if snap == nil {
		return event
	}
	if event.Payload == nil {
		event.Payload = map[string]any{}
	}
	if existing, ok := event.Payload["detail_snapshot"].(map[string]any); ok {
		for k, v := range snap {
			if _, present := existing[k]; !present {
				existing[k] = v
			}
		}
		snap = existing
	}
	event.Payload["detail_snapshot"] = snap
	return event
}

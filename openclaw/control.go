package openclaw

import (
	"context"
	"fmt"
	"strings"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
)

// This file implements the writable half of the unified capability contract
// (docs/06 §5.5): SessionController.ControlSession and ApprovalResolver.
// ResolveApproval. Each control action maps to gateway RPCs:
//
//   model.switch      -> sessions.patch { model } -> resolved.model
//   reasoning.switch  -> sessions.patch { thinkingLevel } -> resolved.thinkingLevel
//   context.compact   -> sessions.compact
//   rename            -> sessions.patch { label }
//   pin               -> sessions.patch { pinned }
//   archive           -> sessions.patch { archived }
//   delete            -> sessions.delete
//
// After a successful mutation the refreshed detail_snapshot is both returned
// synchronously (ControlSessionResult.Details) and re-emitted via the
// Subscribe loop on the next event tick.

// normalizeAction lowercases and accepts both "model.switch" and "model_switch".
func normalizeAction(action string) string {
	a := strings.ToLower(strings.TrimSpace(action))
	a = strings.ReplaceAll(a, "_", ".")
	// Collapse accidental double dots from "model_.switch".
	a = strings.ReplaceAll(a, "..", ".")
	return a
}

// ControlSession implements pluginbridge.SessionController.
func (a *Adapter) ControlSession(ctx context.Context, req pluginbridge.ControlSessionRequest) (pluginbridge.ControlSessionResult, error) {
	sessionKey := strings.TrimSpace(req.Session.NativeSessionID)
	if sessionKey == "" {
		return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: session key required for control")
	}
	action := normalizeAction(req.Action)
	if action == "controls.describe" || action == "describe.controls" {
		return a.controlDescribe(ctx, req.Session)
	}

	client, err := a.dialClient(ctx)
	if err != nil {
		return pluginbridge.ControlSessionResult{}, err
	}
	defer client.Close()

	result := pluginbridge.ControlSessionResult{
		OK:     true,
		Action: action,
	}
	refreshDetail := true
	mutated := false

	switch action {
	case "conversation.select":
		// OpenClaw sessions are gateway-owned; "select" is a no-op identity
		// confirmation since there is no foreground/desktop notion to switch.
		result.Message = "OpenClaw 会话已确认。"
	case "model.switch":
		target := normalizeOptionTarget(req.Target)
		if target == "" {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: model.switch requires a target model id")
		}
		models, err := a.modelsList(ctx, client, "")
		if err != nil || !optionKeyExists(modelsToOptions(models), target) {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: model target is not in the current Gateway catalog: %s", target)
		}
		if err := a.patchSession(ctx, client, sessionKey, map[string]any{"model": target}); err != nil {
			return pluginbridge.ControlSessionResult{}, err
		}
		mutated = true
		result.Message = fmt.Sprintf("已切换模型为 %s。", target)
	case "reasoning.switch":
		target := normalizeOptionTarget(req.Target)
		if target == "" {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: reasoning.switch requires a target level")
		}
		row, err := a.sessionDescribe(ctx, client, sessionKey)
		if err != nil {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: read current reasoning options: %w", err)
		}
		models, modelsErr := a.modelsList(ctx, client, "")
		options := reasoningOptionsFromRow(row)
		if modelsErr == nil {
			options = reasoningOptionsForModel(row, models)
		}
		if !optionKeyExists(options, target) {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: reasoning target is not supported by the current model: %s", target)
		}
		if err := a.patchSession(ctx, client, sessionKey, map[string]any{"thinkingLevel": target}); err != nil {
			return pluginbridge.ControlSessionResult{}, err
		}
		mutated = true
		result.Message = fmt.Sprintf("已切换推理级别为 %s。", target)
	case "context.compact", "compact":
		result.Action = "context.compact"
		if _, err := client.Request(ctx, "sessions.compact", map[string]any{"key": sessionKey}); err != nil {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: sessions.compact failed: %w", err)
		}
		mutated = true
		result.Message = "已完成上下文压缩。"
	case "rename":
		name := strings.TrimSpace(req.Name)
		if name == "" {
			// Some callers pass the new title via Target.
			name = normalizeOptionTarget(req.Target)
		}
		if name == "" {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: rename requires a new name")
		}
		if err := a.patchSession(ctx, client, sessionKey, map[string]any{"label": name}); err != nil {
			return pluginbridge.ControlSessionResult{}, err
		}
		mutated = true
		result.Message = "已重命名会话。"
	case "pin":
		pinned, explicit := boolTarget(req.Target)
		if !explicit {
			row, err := a.sessionDescribe(ctx, client, sessionKey)
			if err != nil {
				return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: read current pin state: %w", err)
			}
			pinned = !boolFromAny(row["pinned"])
		}
		if err := a.patchSession(ctx, client, sessionKey, map[string]any{"pinned": pinned}); err != nil {
			return pluginbridge.ControlSessionResult{}, err
		}
		mutated = true
		if pinned {
			result.Message = "已置顶会话。"
		} else {
			result.Message = "已取消置顶会话。"
		}
	case "archive":
		if err := a.patchSession(ctx, client, sessionKey, map[string]any{"archived": true}); err != nil {
			return pluginbridge.ControlSessionResult{}, err
		}
		mutated = true
		result.Message = "已归档会话。"
	case "delete":
		if _, err := client.Request(ctx, "sessions.delete", map[string]any{
			"key":              sessionKey,
			"deleteTranscript": true,
		}); err != nil {
			return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: sessions.delete failed: %w", err)
		}
		mutated = true
		refreshDetail = false
		result.Message = "已删除会话。"
	default:
		return pluginbridge.ControlSessionResult{}, fmt.Errorf("openclaw: unsupported control action: %s", action)
	}

	result.ThreadID = sessionKey
	result.ConversationID = sessionKey

	// Refresh and attach the detail snapshot so the Hub/mobile sees the new
	// state synchronously. The Subscribe loop will also push a
	// desktop.state.changed event; this Details field is its mirror.
	if refreshDetail {
		if snap, err := a.buildDetailSnapshot(ctx, client, req.Session); err == nil {
			result.Details = snap
		}
	}
	result.DetailsConfirmed = mutated
	return result, nil
}

// controlDescribe returns the current controls without mutating anything.
func (a *Adapter) controlDescribe(ctx context.Context, session pluginbridge.NativeSession) (pluginbridge.ControlSessionResult, error) {
	client, err := a.dialClient(ctx)
	if err != nil {
		return pluginbridge.ControlSessionResult{}, err
	}
	defer client.Close()
	snap, err := a.buildDetailSnapshot(ctx, client, session)
	if err != nil {
		return pluginbridge.ControlSessionResult{}, err
	}
	return pluginbridge.ControlSessionResult{
		OK:      true,
		Action:  "controls.describe",
		Details: snap,
	}, nil
}

// patchSession calls sessions.patch and verifies the response. The gateway
// returns the resolved canonical model under resolved.model; callers read it
// from the subsequent buildDetailSnapshot rather than parsing the patch result
// here, to keep one source of truth.
func (a *Adapter) patchSession(ctx context.Context, client rpcClient, sessionKey string, patch map[string]any) error {
	params := map[string]any{"key": sessionKey}
	for k, v := range patch {
		params[k] = v
	}
	if _, err := client.Request(ctx, "sessions.patch", params); err != nil {
		return fmt.Errorf("openclaw: sessions.patch failed: %w", err)
	}
	return nil
}

// ResolveApproval implements pluginbridge.ApprovalResolver.
func (a *Adapter) ResolveApproval(ctx context.Context, req pluginbridge.ApprovalResolutionRequest) error {
	approvalID := strings.TrimSpace(req.ApprovalRequestID)
	if approvalID == "" {
		return fmt.Errorf("openclaw: approval request id required")
	}
	client, err := a.dialClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	decision, kind := decodeApprovalActionID(req.ActionID)
	if decision == "" {
		return fmt.Errorf("openclaw: could not derive approval decision from action %q", req.ActionID)
	}
	method := kind + ".approval.resolve"
	if _, err := client.Request(ctx, method, map[string]any{
		"id":       approvalID,
		"decision": decision,
	}); err != nil {
		return fmt.Errorf("openclaw: %s failed: %w", method, err)
	}
	return nil
}

// decodeApprovalActionID turns a mobile action id (format "gateway:<i>:<decision>"
// produced by approvalSnapshotFromGet, or a bare decision) into the (decision,
// kind) pair the gateway expects. kind defaults to "exec" which is the common
// case; plugin/system-agent approvals would need richer classification later.
func decodeApprovalActionID(actionID string) (decision, kind string) {
	s := strings.TrimSpace(actionID)
	if s == "" {
		return "", ""
	}
	// Form: gateway:<kind>:<decision>
	if parts := strings.Split(s, ":"); len(parts) >= 3 && parts[0] == "gateway" {
		k := strings.ToLower(strings.TrimSpace(parts[len(parts)-2]))
		d := strings.ToLower(strings.TrimSpace(parts[len(parts)-1]))
		if (k == "exec" || k == "plugin") && isValidDecision(d) {
			return d, k
		}
	}
	d := strings.ToLower(s)
	if isValidDecision(d) {
		return d, "exec"
	}
	return "", ""
}

func optionKeyExists(options []any, target string) bool {
	target = strings.TrimSpace(target)
	for _, raw := range options {
		option, _ := raw.(map[string]any)
		if strings.TrimSpace(stringFromAny(option["key"])) == target {
			return true
		}
	}
	return false
}

func isValidDecision(d string) bool {
	switch d {
	case "allow-once", "allow-always", "deny":
		return true
	}
	return false
}

// normalizeOptionTarget coerces the Target value (which may arrive as a string,
// an option object {key/value}, or a number) into a plain string id.
func normalizeOptionTarget(target any) string {
	if target == nil {
		return ""
	}
	switch v := target.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]any:
		for _, k := range []string{"option_id", "key", "value", "id", "label", "displayName"} {
			if s := strings.TrimSpace(stringFromAny(v[k])); s != "" {
				return s
			}
		}
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	}
	return strings.TrimSpace(fmt.Sprint(target))
}

// boolTarget interprets an explicit pin/unpin target. Session action menus may
// send only action=pin, in which case the adapter reads and toggles native state.
func boolTarget(target any) (bool, bool) {
	if target == nil {
		return false, false
	}
	switch v := target.(type) {
	case bool:
		return v, true
	case map[string]any:
		if enabled, ok := v["enabled"].(bool); ok {
			return enabled, true
		}
	case map[string]string:
		return boolTarget(v["enabled"])
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "pin", "on":
			return true, true
		case "0", "false", "no", "unpin", "off":
			return false, true
		}
	}
	return false, false
}

package openclaw

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	gatewayRequestTimeout = 30 * time.Second
	gatewayDialTimeout    = 8 * time.Second
)

type gatewayClientConfig struct {
	Endpoint string
	Token    string
}

// rpcClient is the subset of *gatewayClient used by the adapter. Defining it
// as an interface lets tests inject a fake client without spinning up a real
// websocket server. *gatewayClient satisfies it.
type rpcClient interface {
	Request(ctx context.Context, method string, params any) (map[string]any, error)
	RequestWithTimeout(ctx context.Context, method string, params any, fallback time.Duration) (map[string]any, error)
	RequestAny(ctx context.Context, method string, params any) (any, error)
	RequestAnyWithTimeout(ctx context.Context, method string, params any, fallback time.Duration) (any, error)
	NextEventWithTimeout(ctx context.Context, fallback time.Duration) (gatewayEventFrame, error)
	Close() error
}

// dialer opens an authenticated rpcClient against the gateway. The default is
// newGatewayClient; tests substitute a fake.
type dialer func(ctx context.Context, cfg gatewayClientConfig) (rpcClient, error)

type gatewayClient struct {
	conn *websocket.Conn

	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[string]chan gatewayResponseFrame

	eventsMu    sync.Mutex
	events      []gatewayEventFrame
	eventSignal chan struct{}

	done      chan struct{}
	closeOnce sync.Once
	errMu     sync.Mutex
	readErr   error
}

type gatewayRequestFrame struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type gatewayResponseFrame struct {
	Type    string                `json:"type"`
	ID      string                `json:"id"`
	OK      bool                  `json:"ok"`
	Payload any                   `json:"payload,omitempty"`
	Error   *gatewayResponseError `json:"error,omitempty"`
}

type gatewayResponseError struct {
	Code         string `json:"code,omitempty"`
	Message      string `json:"message,omitempty"`
	Retryable    bool   `json:"retryable,omitempty"`
	RetryAfterMs int    `json:"retryAfterMs,omitempty"`
}

type gatewayEventFrame struct {
	Type    string         `json:"type"`
	Event   string         `json:"event"`
	Payload map[string]any `json:"payload,omitempty"`
	Seq     int            `json:"seq,omitempty"`
}

func newGatewayClient(ctx context.Context, cfg gatewayClientConfig) (*gatewayClient, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("openclaw gateway endpoint required")
	}
	token := strings.TrimSpace(cfg.Token)
	if token == "" {
		return nil, fmt.Errorf("openclaw gateway token required")
	}
	wsURL, err := websocketURL(endpoint)
	if err != nil {
		return nil, err
	}
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: gatewayDialTimeout,
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		status := ""
		if resp != nil {
			status = " (" + resp.Status + ")"
		}
		return nil, fmt.Errorf("openclaw gateway websocket dial failed%s: %w", status, err)
	}
	client := &gatewayClient{
		conn:        conn,
		pending:     map[string]chan gatewayResponseFrame{},
		eventSignal: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	if err := client.waitConnectChallenge(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Time{})
	go client.readLoop()
	if _, err := client.Request(ctx, "connect", connectParams(token)); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("openclaw gateway connect failed: %w", err)
	}
	return client, nil
}

func (c *gatewayClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.shutdown(nil)
	return nil
}

func (c *gatewayClient) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	return c.RequestWithTimeout(ctx, method, params, gatewayRequestTimeout)
}

func (c *gatewayClient) RequestWithTimeout(ctx context.Context, method string, params any, fallback time.Duration) (map[string]any, error) {
	payload, err := c.RequestAnyWithTimeout(ctx, method, params, fallback)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return map[string]any{}, nil
	}
	result, ok := payload.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("openclaw gateway %s returned %T, expected object", method, payload)
	}
	return result, nil
}

func (c *gatewayClient) RequestAny(ctx context.Context, method string, params any) (any, error) {
	return c.RequestAnyWithTimeout(ctx, method, params, gatewayRequestTimeout)
}

func (c *gatewayClient) RequestAnyWithTimeout(ctx context.Context, method string, params any, fallback time.Duration) (any, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("openclaw gateway client closed")
	}
	method = strings.TrimSpace(method)
	if method == "" {
		return nil, fmt.Errorf("openclaw gateway method required")
	}
	id := uuid.NewString()
	response := make(chan gatewayResponseFrame, 1)
	c.pendingMu.Lock()
	c.pending[id] = response
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	frame := gatewayRequestFrame{
		Type:   "req",
		ID:     id,
		Method: method,
		Params: params,
	}
	c.writeMu.Lock()
	err := c.conn.WriteJSON(frame)
	c.writeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("openclaw gateway write %s: %w", method, err)
	}
	timeout := effectiveTimeout(ctx, fallback)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-response:
		if !res.OK {
			return nil, gatewayError(method, res.Error)
		}
		return res.Payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("openclaw gateway %s timed out after %s", method, timeout)
	case <-c.done:
		return nil, c.connectionError(method)
	}
}

func (c *gatewayClient) NextEvent(ctx context.Context) (gatewayEventFrame, error) {
	return c.NextEventWithTimeout(ctx, 10*time.Minute)
}

func (c *gatewayClient) NextEventWithTimeout(ctx context.Context, fallback time.Duration) (gatewayEventFrame, error) {
	if c == nil || c.conn == nil {
		return gatewayEventFrame{}, fmt.Errorf("openclaw gateway client closed")
	}
	timeout := effectiveTimeout(ctx, fallback)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if event, ok := c.popEvent(); ok {
			return event, nil
		}
		select {
		case <-c.eventSignal:
		case <-ctx.Done():
			return gatewayEventFrame{}, ctx.Err()
		case <-timer.C:
			return gatewayEventFrame{}, fmt.Errorf("openclaw gateway event wait timed out after %s", timeout)
		case <-c.done:
			if event, ok := c.popEvent(); ok {
				return event, nil
			}
			return gatewayEventFrame{}, c.connectionError("event stream")
		}
	}
}

func (c *gatewayClient) readLoop() {
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			c.shutdown(fmt.Errorf("openclaw gateway read: %w", err))
			return
		}
		var head struct {
			Type string `json:"type"`
			ID   string `json:"id,omitempty"`
		}
		if json.Unmarshal(data, &head) != nil {
			continue
		}
		switch head.Type {
		case "res":
			var res gatewayResponseFrame
			if json.Unmarshal(data, &res) != nil {
				continue
			}
			c.pendingMu.Lock()
			waiter := c.pending[res.ID]
			c.pendingMu.Unlock()
			if waiter != nil {
				select {
				case waiter <- res:
				default:
				}
			}
		case "event":
			var event gatewayEventFrame
			if json.Unmarshal(data, &event) != nil {
				continue
			}
			if !c.pushEvent(event) {
				c.shutdown(fmt.Errorf("openclaw gateway event buffer exceeded"))
				return
			}
		}
	}
}

func (c *gatewayClient) pushEvent(event gatewayEventFrame) bool {
	const maxBufferedEvents = 4096
	c.eventsMu.Lock()
	if len(c.events) >= maxBufferedEvents {
		c.eventsMu.Unlock()
		return false
	}
	c.events = append(c.events, event)
	c.eventsMu.Unlock()
	select {
	case c.eventSignal <- struct{}{}:
	default:
	}
	return true
}

func (c *gatewayClient) popEvent() (gatewayEventFrame, bool) {
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	if len(c.events) == 0 {
		return gatewayEventFrame{}, false
	}
	event := c.events[0]
	copy(c.events, c.events[1:])
	c.events[len(c.events)-1] = gatewayEventFrame{}
	c.events = c.events[:len(c.events)-1]
	if len(c.events) > 0 {
		select {
		case c.eventSignal <- struct{}{}:
		default:
		}
	}
	return event, true
}

func (c *gatewayClient) shutdown(err error) {
	c.closeOnce.Do(func() {
		c.errMu.Lock()
		c.readErr = err
		c.errMu.Unlock()
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *gatewayClient) connectionError(operation string) error {
	c.errMu.Lock()
	err := c.readErr
	c.errMu.Unlock()
	if err != nil {
		return fmt.Errorf("openclaw gateway %s failed: %w", operation, err)
	}
	return fmt.Errorf("openclaw gateway %s failed: client closed", operation)
}

func effectiveTimeout(ctx context.Context, fallback time.Duration) time.Duration {
	if fallback <= 0 {
		fallback = gatewayRequestTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < fallback {
			return remaining
		}
	}
	return fallback
}

func (c *gatewayClient) waitConnectChallenge(ctx context.Context) error {
	for {
		if err := c.setReadDeadline(ctx, gatewayDialTimeout); err != nil {
			return err
		}
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("openclaw gateway challenge read: %w", err)
		}
		var event gatewayEventFrame
		if err := json.Unmarshal(data, &event); err != nil {
			continue
		}
		if event.Type == "event" && event.Event == "connect.challenge" {
			return nil
		}
	}
}

func (c *gatewayClient) setReadDeadline(ctx context.Context, fallback time.Duration) error {
	deadline := time.Now().Add(fallback)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("openclaw gateway set read deadline: %w", err)
	}
	return nil
}

func connectParams(token string) map[string]any {
	return map[string]any{
		"minProtocol": 4,
		"maxProtocol": 4,
		"client": map[string]any{
			"id":          "gateway-client",
			"displayName": "Prism",
			"version":     "0.1.0",
			"platform":    runtime.GOOS,
			"mode":        "backend",
			"instanceId":  "prism",
		},
		"caps":   []string{"tool-events"},
		"role":   "operator",
		"scopes": []string{"operator.read", "operator.write", "operator.admin", "operator.approvals"},
		"auth": map[string]any{
			"token": token,
		},
	}
}

func websocketURL(endpoint string) (string, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	switch {
	case strings.HasPrefix(endpoint, "ws://"), strings.HasPrefix(endpoint, "wss://"):
		return endpoint, nil
	case strings.HasPrefix(endpoint, "http://"):
		return "ws://" + strings.TrimPrefix(endpoint, "http://"), nil
	case strings.HasPrefix(endpoint, "https://"):
		return "wss://" + strings.TrimPrefix(endpoint, "https://"), nil
	default:
		return "", fmt.Errorf("openclaw gateway endpoint must be http(s) or ws(s): %s", endpoint)
	}
}

func gatewayError(method string, errShape *gatewayResponseError) error {
	if errShape == nil {
		return fmt.Errorf("openclaw gateway %s failed", method)
	}
	msg := strings.TrimSpace(errShape.Message)
	if msg == "" {
		msg = "request failed"
	}
	if errShape.Code != "" {
		return fmt.Errorf("openclaw gateway %s failed [%s]: %s", method, errShape.Code, msg)
	}
	return fmt.Errorf("openclaw gateway %s failed: %s", method, msg)
}

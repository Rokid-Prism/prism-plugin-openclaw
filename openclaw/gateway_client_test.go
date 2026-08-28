package openclaw

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestGatewayClientDoesNotDropEventsInterleavedWithRPCResponses(t *testing.T) {
	serverErrors := make(chan error, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listener unavailable: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		if err := conn.WriteJSON(gatewayEventFrame{Type: "event", Event: "connect.challenge"}); err != nil {
			serverErrors <- err
			return
		}
		var connect gatewayRequestFrame
		if err := conn.ReadJSON(&connect); err != nil {
			serverErrors <- err
			return
		}
		if err := conn.WriteJSON(gatewayEventFrame{Type: "event", Event: "sessions.changed", Seq: 1}); err != nil {
			serverErrors <- err
			return
		}
		if err := conn.WriteJSON(gatewayResponseFrame{Type: "res", ID: connect.ID, OK: true, Payload: map[string]any{"connected": true}}); err != nil {
			serverErrors <- err
			return
		}

		var request gatewayRequestFrame
		if err := conn.ReadJSON(&request); err != nil {
			serverErrors <- err
			return
		}
		if err := conn.WriteJSON(gatewayEventFrame{Type: "event", Event: "session.message", Seq: 2}); err != nil {
			serverErrors <- err
			return
		}
		if err := conn.WriteJSON(gatewayResponseFrame{Type: "res", ID: request.ID, OK: true, Payload: map[string]any{"value": "ok"}}); err != nil {
			serverErrors <- err
		}
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := newGatewayClient(ctx, gatewayClientConfig{
		Endpoint: strings.Replace(server.URL, "http://", "ws://", 1),
		Token:    "test-token",
	})
	if err != nil {
		t.Fatalf("newGatewayClient: %v", err)
	}
	defer client.Close()
	payload, err := client.Request(ctx, "test.echo", map[string]any{})
	if err != nil || payload["value"] != "ok" {
		t.Fatalf("Request payload=%+v err=%v", payload, err)
	}
	first, err := client.NextEventWithTimeout(ctx, time.Second)
	if err != nil || first.Event != "sessions.changed" || first.Seq != 1 {
		t.Fatalf("first event=%+v err=%v", first, err)
	}
	second, err := client.NextEventWithTimeout(ctx, time.Second)
	if err != nil || second.Event != "session.message" || second.Seq != 2 {
		t.Fatalf("second event=%+v err=%v", second, err)
	}
	select {
	case err := <-serverErrors:
		t.Fatalf("server: %v", err)
	default:
	}
}

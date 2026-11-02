//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Exactly two owned numeric connections, each with one HTTP request followed
// by actual EOF. This is offline transport evidence, not model qualification.
type offlineModelChainPeer struct {
	listener  *net.TCPListener
	mu        sync.Mutex
	conn      *net.TCPConn
	closed    bool
	response  [2][]byte
	result    chan []localLoopbackPeerResult
	done      chan struct{}
	want      int
	completed int
	changed   chan struct{}
}

func newOfflineModelChainPeer(t *testing.T, expected ...int) *offlineModelChainPeer {
	t.Helper()
	want := 2
	if len(expected) > 1 {
		t.Fatal("one closed MODEL-count selection only")
	}
	if len(expected) == 1 {
		want = expected[0]
	}
	if want != 1 && want != 2 {
		t.Fatal("MODEL-count must be 1 or 2")
	}
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal("owned two-MODEL listener", err)
	}
	p := &offlineModelChainPeer{listener: l, result: make(chan []localLoopbackPeerResult, 1), done: make(chan struct{})}
	p.want = want
	p.changed = make(chan struct{}, 2)
	t.Cleanup(func() {
		p.mu.Lock()
		p.closed = true
		c := p.conn
		p.mu.Unlock()
		_ = l.Close()
		if c != nil {
			_ = c.Close()
		}
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
		case <-timer.C:
			select {
			case <-p.done:
			default:
				t.Error("two-MODEL peer failed to join after owned sockets closed")
			}
		}
		for _, b := range p.response {
			clear(b)
		}
	})
	go p.serve()
	return p
}

func (p *offlineModelChainPeer) endpoint() string {
	return "http://" + p.listener.Addr().String() + "/v1"
}

func (p *offlineModelChainPeer) configure(t *testing.T, model string) {
	t.Helper()
	first, err := json.Marshal(map[string]any{"model": model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "offline-call-1", "type": "function", "function": map[string]any{"name": "search_openai_docs", "arguments": `{"query":"public docs"}`}}}}, "finish_reason": "tool_calls"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
	if err != nil {
		t.Fatal("first MODEL response", err)
	}
	final, err := json.Marshal(map[string]any{"model": model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "offline joined final answer"}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
	if err != nil {
		t.Fatal("final MODEL response", err)
	}
	p.mu.Lock()
	p.response = [2][]byte{first, final}
	p.mu.Unlock()
}

func (p *offlineModelChainPeer) serve() {
	defer close(p.done)
	defer p.listener.Close()
	var results []localLoopbackPeerResult
	defer func() { p.result <- results }()
	for i := range p.want {
		c, err := p.listener.AcceptTCP()
		if err != nil {
			results = append(results, localLoopbackPeerResult{err: err})
			return
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = c.Close()
			results = append(results, localLoopbackPeerResult{err: net.ErrClosed})
			return
		}
		p.conn = c
		response := bytes.Clone(p.response[i])
		p.mu.Unlock()
		r := readOfflineModelRequest(c, response)
		clear(response)
		_ = c.Close()
		p.mu.Lock()
		p.conn = nil
		p.mu.Unlock()
		results = append(results, r)
		p.mu.Lock()
		p.completed++
		p.mu.Unlock()
		p.changed <- struct{}{}
		if r.err != nil {
			return
		}
	}
}
func (p *offlineModelChainPeer) completedCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.completed
}
func (p *offlineModelChainPeer) waitCompleted(t *testing.T, want int) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		if p.completedCalls() >= want {
			return
		}
		select {
		case <-p.changed:
		case <-timer.C:
			if p.completedCalls() < want {
				t.Fatalf("MODEL completed calls=%d want=%d", p.completedCalls(), want)
			}
			return
		}
	}
}

func readOfflineModelRequest(c *net.TCPConn, response []byte) localLoopbackPeerResult {
	if err := c.SetDeadline(time.Now().Add(25 * time.Second)); err != nil {
		return localLoopbackPeerResult{err: err}
	}
	reader := bufio.NewReader(io.LimitReader(c, (1<<20)+(16<<10)+1))
	r, err := http.ReadRequest(reader)
	if err != nil {
		return localLoopbackPeerResult{err: err}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	_ = r.Body.Close()
	result := localLoopbackPeerResult{request: r, body: body, err: err}
	if err != nil || len(body) > 1<<20 || len(response) == 0 {
		result.err = errors.New("two-MODEL peer requires bounded configured request/response")
		return result
	}
	if _, err = fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n", len(response)); err == nil {
		var n int
		n, err = c.Write(response)
		if err == nil && n != len(response) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		result.extra, err = io.ReadAll(io.LimitReader(reader, 1025))
	}
	result.err = err
	return result
}

func (p *offlineModelChainPeer) wait(t *testing.T, bearer bool) []localLoopbackPeerResult {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	var result []localLoopbackPeerResult
	select {
	case result = <-p.result:
	case <-timer.C:
		select {
		case result = <-p.result:
		default:
			t.Fatal("two actual MODEL requests did not reach EOF/join")
		}
	}
	if len(result) != p.want {
		t.Fatalf("MODEL requests=%d want=%d; result=%+v", len(result), p.want, result)
	}
	for i, r := range result {
		if r.err != nil || r.request == nil || len(r.extra) != 0 {
			t.Fatalf("MODEL%d request/EOF: err=%v request=%t extra=%d", i+1, r.err, r.request != nil, len(r.extra))
		}
		want := ""
		if bearer {
			want = "Bearer " + localSessionFixtureCredential
		}
		if r.request.Method != "POST" || r.request.RequestURI != "/v1/chat/completions" || r.request.Host != p.listener.Addr().String() || !r.request.Close || r.request.Header.Get("Authorization") != want {
			t.Fatalf("MODEL%d fixed wire/auth changed", i+1)
		}
		if !bearer {
			if _, present := r.request.Header["Authorization"]; present {
				t.Fatalf("noauth MODEL%d carried Authorization", i+1)
			}
		}
		t.Cleanup(func() { clear(r.body); clear(r.extra) })
	}
	return result
}

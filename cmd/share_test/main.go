// Command share_test 只读分享链接功能的端到端验证
// 前置条件: 服务端已在 localhost:8080 运行
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type Msg struct {
	Type     string      `json:"type"`
	DocID    string      `json:"doc_id,omitempty"`
	ClientID string      `json:"client_id,omitempty"`
	Username string      `json:"username,omitempty"`
	Version  int64       `json:"version,omitempty"`
	BaseVer  int64       `json:"base_version,omitempty"`
	Op       interface{} `json:"op,omitempty"`
	Content  string      `json:"content,omitempty"`
	Users    []struct {
		ClientID string `json:"client_id"`
		Username string `json:"username"`
	} `json:"users,omitempty"`
	ReadOnly bool   `json:"read_only,omitempty"`
	Error    string `json:"error,omitempty"`
}

var failed bool

func check(name string, ok bool, detail string) {
	if ok {
		fmt.Printf("  ✅ %s %s\n", name, detail)
	} else {
		fmt.Printf("  ❌ %s %s\n", name, detail)
		failed = true
	}
}

func main() {
	// 1. 创建文档
	docID, err := createDoc()
	if err != nil {
		fmt.Printf("创建文档失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✅ 文档已创建: %s\n\n", docID)

	// 2. 编辑者连接并写入内容
	editor, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://localhost:8080/ws?doc_id=%s&username=Editor", docID), nil)
	if err != nil {
		fmt.Printf("编辑者连接失败: %v\n", err)
		os.Exit(1)
	}
	defer editor.Close()
	editorInit := readInit(editor, "Editor")
	fmt.Printf("✅ 编辑者已连接 version=%d\n\n", editorInit.Version)

	sendInsert(editor, 0, "Hello 只读分享")
	time.Sleep(400 * time.Millisecond)

	// 3. 创建永不过期的分享链接
	fmt.Println("=== 测试1: 创建分享链接 ===")
	token, err := createShare(docID, 0)
	if err != nil {
		fmt.Printf("创建分享链接失败: %v\n", err)
		os.Exit(1)
	}
	check("创建链接", token != "", fmt.Sprintf("token=%s...", token[:8]))

	// 4. 访客通过token获取只读快照
	fmt.Println("\n=== 测试2: 访客HTTP只读访问 ===")
	share, code := getShare(token)
	check("快照可访问", code == 200, fmt.Sprintf("code=%d", code))
	check("快照内容一致", share["content"] == "Hello 只读分享", fmt.Sprintf("content=%q", share["content"]))
	check("快照标记只读", share["read_only"] == true, "")

	// 5. 访客WebSocket连接
	fmt.Println("\n=== 测试3: 访客WebSocket实时视图 ===")
	viewer, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://localhost:8080/ws/share?token=%s", token), nil)
	if err != nil {
		fmt.Printf("访客连接失败: %v\n", err)
		os.Exit(1)
	}
	defer viewer.Close()
	vinit := readInit(viewer, "Viewer")
	check("init内容为最新", vinit.Content == "Hello 只读分享", fmt.Sprintf("content=%q", vinit.Content))
	check("init标记只读", vinit.ReadOnly, "")
	check("访客看不到自己", !hasUser(vinit, "Viewer"), fmt.Sprintf("users=%d", len(vinit.Users)))

	// 编辑者不应在用户列表里看到访客
	time.Sleep(200 * time.Millisecond)
	editor2, _, _ := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://localhost:8080/ws?doc_id=%s&username=Editor2", docID), nil)
	defer editor2.Close()
	e2init := readInit(editor2, "Editor2")
	check("协作者列表不含访客", !hasUser(e2init, "访客"), fmt.Sprintf("users=%v", userNames(e2init)))

	// 后台收集编辑者收到的消息（验证访客光标不外泄）
	editorGotCursorFromViewer := make(chan bool, 1)
	go func() {
		for {
			var m Msg
			if err := editor.ReadJSON(&m); err != nil {
				return
			}
			if m.Type == "cursor_move" && strings.HasPrefix(m.Username, "访客") {
				editorGotCursorFromViewer <- true
			}
		}
	}()

	// 6. 编辑者继续编辑，访客应实时收到op
	fmt.Println("\n=== 测试4: 访客实时收到内容更新 ===")
	sendInsert(editor, 8, "!")
	viewer.SetReadDeadline(time.Now().Add(2 * time.Second))
	gotOp := false
	for i := 0; i < 5; i++ {
		var m Msg
		if err := viewer.ReadJSON(&m); err != nil {
			break
		}
		if m.Type == "op" {
			gotOp = true
			check("访客收到op广播", m.Version == vinit.Version+1, fmt.Sprintf("version=%d", m.Version))
			break
		}
	}
	if !gotOp {
		check("访客收到op广播", false, "超时未收到")
	}

	// 7. 编辑者移动光标，访客应看到光标变化
	fmt.Println("\n=== 测试5: 访客实时看到协作者光标 ===")
	editor.WriteJSON(map[string]interface{}{"type": "cursor_move", "position": 5})
	viewer.SetReadDeadline(time.Now().Add(2 * time.Second))
	gotCursor := false
	for i := 0; i < 5; i++ {
		var m Msg
		if err := viewer.ReadJSON(&m); err != nil {
			break
		}
		if m.Type == "cursor_move" && m.Username == "Editor" {
			gotCursor = true
			check("访客收到光标广播", true, fmt.Sprintf("pos=%d user=%s", 5, m.Username))
			break
		}
	}
	if !gotCursor {
		check("访客收到光标广播", false, "超时未收到")
	}

	// 8. 访客尝试发送op和光标（恶意），不应产生任何影响
	fmt.Println("\n=== 测试6: 访客上行消息被完全忽略 ===")
	snapBefore, _ := getSnapshot(docID)
	viewer.WriteJSON(map[string]interface{}{
		"type": "op",
		"op":   map[string]interface{}{"type": "insert", "position": 0, "text": "HACK"},
	})
	viewer.WriteJSON(map[string]interface{}{"type": "cursor_move", "position": 3})
	time.Sleep(600 * time.Millisecond)
	snapAfter, _ := getSnapshot(docID)
	check("文档内容未被篡改", snapBefore["content"] == snapAfter["content"],
		fmt.Sprintf("content=%q", snapAfter["content"]))
	check("版本号未变化", snapBefore["version"] == snapAfter["version"],
		fmt.Sprintf("version=%v", snapAfter["version"]))
	select {
	case <-editorGotCursorFromViewer:
		check("访客光标不外泄", false, "编辑者收到了访客光标")
	case <-time.After(300 * time.Millisecond):
		check("访客光标不外泄", true, "")
	}

	// 9. 撤销链接：访客立刻被断开
	fmt.Println("\n=== 测试7: 撤销后访客立刻失去访问 ===")
	if err := revokeShare(docID, token); err != nil {
		fmt.Printf("撤销失败: %v\n", err)
		os.Exit(1)
	}
	viewer.SetReadDeadline(time.Now().Add(3 * time.Second))
	gotRevoked := false
	connClosed := false
	for {
		var m Msg
		if err := viewer.ReadJSON(&m); err != nil {
			connClosed = true
			break
		}
		if m.Type == "share_revoked" {
			gotRevoked = true
		}
	}
	check("访客收到撤销通知", gotRevoked, "")
	check("访客连接被关闭", connClosed, "")

	_, code = getShare(token)
	check("撤销后快照不可访问", code == 404, fmt.Sprintf("code=%d", code))

	_, _, err = websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://localhost:8080/ws/share?token=%s", token), nil)
	check("撤销后WS连接被拒绝", err != nil, "")

	// 10. 有效期：2秒后过期
	fmt.Println("\n=== 测试8: 链接有效期 ===")
	token2, err := createShare(docID, 2)
	if err != nil {
		fmt.Printf("创建限时链接失败: %v\n", err)
		os.Exit(1)
	}
	viewer2, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://localhost:8080/ws/share?token=%s", token2), nil)
	if err != nil {
		fmt.Printf("访客2连接失败: %v\n", err)
		os.Exit(1)
	}
	readInit(viewer2, "Viewer2")
	check("过期前可正常连接", true, "")

	// 等待过期（2s有效期 + 缓冲）
	viewer2.SetReadDeadline(time.Now().Add(6 * time.Second))
	gotExpired := false
	conn2Closed := false
	for {
		var m Msg
		if err := viewer2.ReadJSON(&m); err != nil {
			conn2Closed = true
			break
		}
		if m.Type == "share_revoked" {
			gotExpired = true
		}
	}
	check("过期后访客收到通知", gotExpired, "")
	check("过期后访客连接被关闭", conn2Closed, "")

	_, code = getShare(token2)
	check("过期后快照不可访问", code == 404, fmt.Sprintf("code=%d", code))

	// 汇总
	fmt.Println("\n=== 测试结果 ===")
	if failed {
		fmt.Println("❌ 存在失败用例")
		os.Exit(1)
	}
	fmt.Println("🎉 全部通过：只读分享链接功能符合需求")
}

func readInit(conn *websocket.Conn, name string) Msg {
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var msg Msg
	if err := conn.ReadJSON(&msg); err != nil {
		fmt.Printf("[%s] 读取init失败: %v\n", name, err)
		return msg
	}
	conn.SetReadDeadline(time.Time{})
	return msg
}

func hasUser(m Msg, prefix string) bool {
	for _, u := range m.Users {
		if strings.HasPrefix(u.Username, prefix) {
			return true
		}
	}
	return false
}

func userNames(m Msg) []string {
	names := make([]string, 0, len(m.Users))
	for _, u := range m.Users {
		names = append(names, u.Username)
	}
	return names
}

func createDoc() (string, error) {
	resp, err := http.Post("http://localhost:8080/api/documents", "application/json",
		strings.NewReader(`{"title":"只读分享测试"}`))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	return result["id"].(string), nil
}

func createShare(docID string, expiresIn int64) (string, error) {
	resp, err := http.Post(
		fmt.Sprintf("http://localhost:8080/api/documents/%s/share", docID),
		"application/json",
		strings.NewReader(fmt.Sprintf(`{"expires_in_seconds": %d}`, expiresIn)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	token, _ := result["token"].(string)
	if token == "" {
		return "", fmt.Errorf("no token in response: %s", string(body))
	}
	return token, nil
}

func getShare(token string) (map[string]interface{}, int) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8080/api/shares/%s", token))
	if err != nil {
		return nil, -1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, resp.StatusCode
}

func revokeShare(docID, token string) error {
	req, _ := http.NewRequest("DELETE",
		fmt.Sprintf("http://localhost:8080/api/documents/%s/share/%s", docID, token), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("revoke failed: %d %s", resp.StatusCode, string(body))
	}
	return nil
}

func sendInsert(conn *websocket.Conn, pos int64, text string) {
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.WriteJSON(map[string]interface{}{
		"type": "op",
		"op":   map[string]interface{}{"type": "insert", "position": pos, "text": text},
	})
}

func getSnapshot(docID string) (map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8080/api/documents/%s/snapshot", docID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

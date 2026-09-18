// Package ws 实现WebSocket Hub和房间管理
// 负责连接管理、操作广播、在线用户光标同步
package ws

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// MessageType 消息类型
type MessageType string

const (
	MsgOp           MessageType = "op"            // 操作消息
	MsgAck          MessageType = "ack"           // 操作确认
	MsgCursors      MessageType = "cursors"       // 光标位置广播
	MsgCursorMove   MessageType = "cursor_move"   // 光标移动
	MsgUserJoin     MessageType = "user_join"     // 用户加入
	MsgUserLeave    MessageType = "user_leave"    // 用户离开
	MsgInit         MessageType = "init"          // 初始化消息
	MsgError        MessageType = "error"         // 错误消息
	MsgSnapshot     MessageType = "snapshot"      // 快照请求
	MsgShareRevoked MessageType = "share_revoked" // 分享链接已撤销
	MsgShareExpired MessageType = "share_expired" // 分享链接已过期
)

// Message WebSocket消息
type Message struct {
	Type      MessageType `json:"type"`
	DocID     string      `json:"doc_id,omitempty"`
	ClientID  string      `json:"client_id,omitempty"`
	Username  string      `json:"username,omitempty"`
	Title     string      `json:"title,omitempty"`
	Version   int64       `json:"version,omitempty"`
	BaseVer   int64       `json:"base_version,omitempty"`
	Op        interface{} `json:"op,omitempty"`
	Content   string      `json:"content,omitempty"`
	Users     []UserInfo  `json:"users,omitempty"`
	Cursors   []Cursor    `json:"cursors,omitempty"`
	Position  int         `json:"position,omitempty"`
	Color     string      `json:"color,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp time.Time   `json:"timestamp,omitempty"`
}

// UserInfo 用户信息
type UserInfo struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Color    string `json:"color"`
}

// Cursor 光标位置
type Cursor struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Position int    `json:"position"`
	Color    string `json:"color"`
}

// Client 表示一个WebSocket连接
type Client struct {
	Hub      *Hub
	RoomID   string
	ClientID string
	Username string
	Color    string
	Position int
	Conn     *websocket.Conn
	Send     chan []byte
	mu       sync.Mutex

	// 只读访客（通过分享链接进入）
	ReadOnly       bool
	ShareToken     string
	ShareExpiresAt *time.Time // nil 表示永不过期
}

// Room 表示一个文档房间
type Room struct {
	ID      string
	Clients map[string]*Client
	mu      sync.RWMutex
}

// Hub 管理所有房间
type Hub struct {
	Rooms      map[string]*Room
	mu         sync.RWMutex
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *BroadcastMessage
}

// BroadcastMessage 广播消息
type BroadcastMessage struct {
	RoomID  string
	Message []byte
	Except  string // 排除的client_id（发送者）
}

// 预定义颜色列表
var colors = []string{
	"#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6",
	"#1abc9c", "#e67e22", "#2980b9", "#27ae60", "#c0392b",
	"#8e44ad", "#d35400", "#16a085", "#2c3e50", "#e84393",
}

// randomColor 随机选颜色
func randomColor() string {
	return colors[int(uuid.New().ID())%len(colors)]
}

// NewHub 创建Hub
func NewHub() *Hub {
	return &Hub{
		Rooms:      make(map[string]*Room),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *BroadcastMessage, 256),
	}
}

// Run 启动Hub主循环
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.addClient(client)
		case client := <-h.Unregister:
			h.removeClient(client)
		case msg := <-h.Broadcast:
			h.broadcast(msg)
		}
	}
}

// addClient 添加客户端到房间
func (h *Hub) addClient(client *Client) {
	h.mu.Lock()
	room, exists := h.Rooms[client.RoomID]
	if !exists {
		room = &Room{ID: client.RoomID, Clients: make(map[string]*Client)}
		h.Rooms[client.RoomID] = room
	}
	h.mu.Unlock()

	room.mu.Lock()
	room.Clients[client.ClientID] = client
	room.mu.Unlock()

	log.Printf("[Hub] Client %s joined room %s (total: %d)", client.ClientID, client.RoomID, len(room.Clients))
}

// removeClient 从房间移除客户端
func (h *Hub) removeClient(client *Client) {
	h.mu.RLock()
	room, exists := h.Rooms[client.RoomID]
	h.mu.RUnlock()
	if !exists {
		return
	}

	room.mu.Lock()
	if _, ok := room.Clients[client.ClientID]; ok {
		delete(room.Clients, client.ClientID)
		close(client.Send)
	}
	isEmpty := len(room.Clients) == 0
	room.mu.Unlock()

	// 如果房间为空，删除
	if isEmpty {
		h.mu.Lock()
		delete(h.Rooms, client.RoomID)
		h.mu.Unlock()
		log.Printf("[Hub] Room %s empty, removed", client.RoomID)
	} else if !client.ReadOnly {
		// 通知其他用户（只读访客对协作者不可见，不广播离开）
		h.broadcastUserLeave(room, client)
		log.Printf("[Hub] Client %s left room %s (remaining: %d)", client.ClientID, client.RoomID, len(room.Clients))
	}
}

// broadcast 发送消息到房间内所有客户端
func (h *Hub) broadcast(msg *BroadcastMessage) {
	h.mu.RLock()
	room, exists := h.Rooms[msg.RoomID]
	h.mu.RUnlock()
	if !exists {
		return
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	for _, client := range room.Clients {
		if client.ClientID == msg.Except {
			continue
		}
		select {
		case client.Send <- msg.Message:
		default:
			log.Printf("[Hub] Client %s send buffer full, dropping message", client.ClientID)
		}
	}
}

// broadcastUserLeave 广播用户离开事件
func (h *Hub) broadcastUserLeave(room *Room, leaving *Client) {
	msg := Message{
		Type:     MsgUserLeave,
		DocID:    room.ID,
		ClientID: leaving.ClientID,
		Username: leaving.Username,
	}
	data, _ := json.Marshal(msg)

	room.mu.RLock()
	defer room.mu.RUnlock()

	for _, client := range room.Clients {
		select {
		case client.Send <- data:
		default:
		}
	}
}

// BroadcastOp 广播操作消息
func (h *Hub) BroadcastOp(roomID string, senderID string, opMsg Message) {
	data, _ := json.Marshal(opMsg)
	h.Broadcast <- &BroadcastMessage{
		RoomID:  roomID,
		Message: data,
		Except:  senderID,
	}
}

// BroadcastCursors 广播光标位置
func (h *Hub) BroadcastCursors(roomID string, senderID string, cursorMsg Message) {
	data, _ := json.Marshal(cursorMsg)
	h.Broadcast <- &BroadcastMessage{
		RoomID:  roomID,
		Message: data,
		Except:  senderID,
	}
}

// GetRoomUsers 获取房间内所有用户信息（不含只读访客）
func (h *Hub) GetRoomUsers(roomID string) []UserInfo {
	h.mu.RLock()
	room, exists := h.Rooms[roomID]
	h.mu.RUnlock()
	if !exists {
		return nil
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	users := make([]UserInfo, 0, len(room.Clients))
	for _, c := range room.Clients {
		if c.ReadOnly {
			continue
		}
		users = append(users, UserInfo{
			ClientID: c.ClientID,
			Username: c.Username,
			Color:    c.Color,
		})
	}
	return users
}

// GetRoomCursors 获取房间内所有光标位置（不含只读访客）
func (h *Hub) GetRoomCursors(roomID string) []Cursor {
	h.mu.RLock()
	room, exists := h.Rooms[roomID]
	h.mu.RUnlock()
	if !exists {
		return nil
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	cursors := make([]Cursor, 0, len(room.Clients))
	for _, c := range room.Clients {
		if c.ReadOnly {
			continue
		}
		cursors = append(cursors, Cursor{
			ClientID: c.ClientID,
			Username: c.Username,
			Position: c.Position,
			Color:    c.Color,
		})
	}
	return cursors
}

// IsRoomEmpty 检查房间是否为空
func (h *Hub) IsRoomEmpty(roomID string) bool {
	h.mu.RLock()
	room, exists := h.Rooms[roomID]
	h.mu.RUnlock()
	if !exists {
		return true
	}
	room.mu.RLock()
	defer room.mu.RUnlock()
	return len(room.Clients) == 0
}

// KickShareViewers 断开指定分享链接的所有访客连接（用于撤销链接）
// 先推送通知消息，短暂延迟后关闭连接，确保消息送达
func (h *Hub) KickShareViewers(token string, msgType MessageType, reason string) int {
	clients := h.findViewers(func(c *Client) bool {
		return c.ReadOnly && c.ShareToken == token
	})
	for _, c := range clients {
		kickViewer(c, msgType, reason)
	}
	return len(clients)
}

// KickExpiredViewers 断开所有已过期的访客连接，返回断开数量
func (h *Hub) KickExpiredViewers(now time.Time) int {
	clients := h.findViewers(func(c *Client) bool {
		return c.ReadOnly && c.ShareExpiresAt != nil && !c.ShareExpiresAt.After(now)
	})
	for _, c := range clients {
		kickViewer(c, MsgShareExpired, "分享链接已过期")
	}
	return len(clients)
}

// findViewers 查找满足条件的访客连接
func (h *Hub) findViewers(match func(*Client) bool) []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var clients []*Client
	for _, room := range h.Rooms {
		room.mu.RLock()
		for _, c := range room.Clients {
			if match(c) {
				clients = append(clients, c)
			}
		}
		room.mu.RUnlock()
	}
	return clients
}

// kickViewer 通知访客链接失效并关闭其连接
func kickViewer(c *Client, msgType MessageType, reason string) {
	_ = c.SendMessage(Message{
		Type:     msgType,
		DocID:    c.RoomID,
		ClientID: c.ClientID,
		Error:    reason,
	})
	// 留出时间让 WritePump 把通知发出去，再关闭连接触发清理
	time.AfterFunc(200*time.Millisecond, func() {
		c.Conn.Close()
	})
}

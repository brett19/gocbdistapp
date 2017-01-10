package cbdistapp

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"gopkg.in/couchbase/gocb.v1"
)

var (
	errDoNotUpdate = errors.New("Do not update data")
)

type jsonNode struct {
	Uuid string `json:"uuid"`
	Ticks uint64 `json:"ticks"`
}

func (s jsonNode) GetUuid() string {
	return s.Uuid
}

type jsonState struct {
	Version    uint64 `json:"version"`
	TickPeriod time.Duration `json:"tickPeriod"`
	Nodes      []jsonNode `json:"nodes"`
	Data       json.RawMessage `json:"data"`
}

func (s jsonState) GetVersion() uint64 {
	return s.Version
}

func (s jsonState) GetNodes() []INode {
	var servers []INode
	for _, i := range s.Nodes {
		servers = append(servers, i)
	}
	return servers
}

func (s jsonState) GetData(data interface{}) error {
	return json.Unmarshal(s.Data, data)
}

func (s *jsonState) SetData(data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	s.Data = bytes
	s.Version++
	return nil
}

type INode interface {
	GetUuid() string
}

type IState interface {
	GetNodes() []INode
	GetData(interface{}) error
}

type IMutableState interface {
	IState

	SetData(interface{}) error
}

type ClusterEventHandler interface {
	NormalizeState(IMutableState)
	StateChanged(IState)
	Disconnected()
}

type clusterSeenNode struct {
	lastSeen time.Time
	lastTick uint64
}

type ClusterManager struct {
	nodeUuid       string
	eventCh        chan interface{}
	cntlBucket     *gocb.Bucket
	handler        ClusterEventHandler
	isRunning      bool
	numFailedTicks uint
	seenNodes      map[string]*clusterSeenNode
	stateCas       gocb.Cas
	state          jsonState
}

func NewClusterManager(cntlBucket *gocb.Bucket, handler ClusterEventHandler) (*ClusterManager, error) {
	nodeUuid := uuid.New().String()

	cluster := &ClusterManager{
		nodeUuid: nodeUuid,
		eventCh: make(chan interface{}, 1024),
		cntlBucket: cntlBucket,
		handler: handler,
		isRunning: false,
		seenNodes: make(map[string]*clusterSeenNode),
		numFailedTicks: 0,
		stateCas: 0,
	}

	return cluster, nil
}

func (m *ClusterManager) GetUuid() string {
	return m.nodeUuid
}

func (m *ClusterManager) Start() error {
	err := m.updateState(func(state *jsonState) error {
		m.updateMyNodeEntry(state)
		return nil
	})
	if err != nil {
		return err
	}

	go m.threadProc()

	return nil
}

func (m *ClusterManager) Close() {
	// TODO: Should wait till shutdown finishes
	close(m.eventCh)
}

type StateUpdateFunc func(*jsonState) error

func makeDefaultState() jsonState {
	return jsonState {
		Version: 0,
		TickPeriod: 2 * time.Second,
		Data: json.RawMessage("{}"),
	}
}

func (m *ClusterManager) KvGet(key string, dataPtr interface{}) error {
	_, err := m.cntlBucket.Get(key, dataPtr)
	return err
}

func (m *ClusterManager) KvInsert(key string, data interface{}) error {
	_, err := m.cntlBucket.Insert(key, data, 0)
	return err
}

type IMutableData []byte

func (d IMutableData) IsEmpty() bool {
	return len(d) == 0
}

func (d IMutableData) Get(dataPtr interface{}) error {
	return json.Unmarshal(d, dataPtr)
}

func (d *IMutableData) Set(data interface{}) error {
	var err error
	*d, err = json.Marshal(data)
	return err
}

type KvUpdateFunc func(data *IMutableData) error

func (m *ClusterManager) KvUpdate(key string, updateFn KvUpdateFunc) error {
	for {
		var data IMutableData
		cas, err := m.cntlBucket.Get(key, &data)
		if err == gocb.ErrKeyNotFound {
			data = nil
		} else if err != nil {
			return err
		}

		err = updateFn(&data)
		if err == errDoNotUpdate {
			return nil
		} else if err != nil {
			return err
		}

		_, err = m.cntlBucket.Replace(key, data, cas, 0)
		if err == gocb.ErrKeyExists {
			continue
		} else if err != nil {
			return err
		}

		break
	}

	return nil
}

func (m *ClusterManager) updateState(updateFn StateUpdateFunc) error {
	state := m.state
	stateCas := m.stateCas

	for {
		var err error

		if stateCas == 0 {
			stateCas, err = m.cntlBucket.Get("state", &state)
			if err == gocb.ErrKeyNotFound {
				state = makeDefaultState()
				stateCas = 0
			} else if err != nil {
				return err
			}
		}

		oldVersion := state.Version

		err = updateFn(&state)
		if err == errDoNotUpdate {
			break
		} else if err != nil {
			return err
		}

		if state.Version != oldVersion {
			m.handler.NormalizeState(&state)
		}

		if stateCas == 0 {
			_, err = m.cntlBucket.Insert("state", &state, 0)
			if err == gocb.ErrKeyExists {
				stateCas = 0
				continue
			} else if err != nil {
				return err
			}
		} else {
			_, err = m.cntlBucket.Replace("state", &state, stateCas, 0)
			if err == gocb.ErrKeyExists || err == gocb.ErrKeyNotFound {
				stateCas = 0
				continue
			} else if err != nil {
				return err
			}
		}

		break
	}

	hasDataChanged := m.state.Version != state.Version

	m.state = state
	m.stateCas = stateCas

	if hasDataChanged || !m.isRunning {
		m.isRunning = true
		m.handler.StateChanged(m.state)
	}

	return nil
}

type DataUpdateFunc func(IMutableState) error

type updateDataEvent struct {
	waitCh chan error
	mutateFn DataUpdateFunc
}

func (m *ClusterManager) Update(mutateFn DataUpdateFunc) error {
	event := updateDataEvent{
		waitCh: make(chan error),
		mutateFn: mutateFn,
	}
	m.eventCh <- &event
	return <- event.waitCh
}

func (m *ClusterManager) procUpdateData(mutateFn DataUpdateFunc) error {
	return m.updateState(func(state *jsonState) error {
		oldVersion := state.Version

		err := mutateFn(state)
		if err != nil {
			return err
		}

		if state.Version == oldVersion {
			return errDoNotUpdate
		}

		return nil
	})
}

func (m *ClusterManager) containsMyNodeEntry(state *jsonState) bool {
	for _, server := range state.Nodes {
		if server.Uuid == m.nodeUuid {
			return true
		}
	}

	return false
}

func (m *ClusterManager) updateMyNodeEntry(state *jsonState) {
	myId := -1

	for i, server := range state.Nodes {
		if server.Uuid == m.nodeUuid {
			myId = i
			break
		}
	}

	var myServer *jsonNode
	if myId == -1 {
		state.Nodes = append(state.Nodes, jsonNode{
			Uuid: m.nodeUuid,
			Ticks: 0,
		})
		myServer = &state.Nodes[len(state.Nodes) - 1]
		state.Version++
	} else {
		myServer = &state.Nodes[myId]
		myServer.Ticks++
	}
}

func (m *ClusterManager) removeMyNodeEntry(state *jsonState) {
	var newNodes []jsonNode

	removedAnything := false

	for _, server := range state.Nodes {
		if server.Uuid != m.nodeUuid {
			newNodes = append(newNodes, server)
		} else {
			removedAnything = true
		}
	}

	if !removedAnything {
		return
	}

	state.Nodes = newNodes
	state.Version++
}

func (m *ClusterManager) deleteDeadNodes(state *jsonState) {
	var aliveNodes []jsonNode
	hasDeadNodes := false

	for _, node := range state.Nodes {
		seenSrv := m.seenNodes[node.Uuid]
		if seenSrv != nil {
			if node.Ticks != seenSrv.lastTick {
				seenSrv.lastSeen = time.Now()
				seenSrv.lastTick = node.Ticks
			}
		} else {
			seenSrv = &clusterSeenNode{
				lastSeen: time.Now(),
				lastTick: node.Ticks,
			}
			m.seenNodes[node.Uuid] = seenSrv
		}

		if time.Now().Sub(seenSrv.lastSeen) > 3 * m.state.TickPeriod {
			// Server is dead
			hasDeadNodes = true
		} else {
			aliveNodes = append(aliveNodes, node)
		}
	}

	if hasDeadNodes {
		state.Nodes = aliveNodes
		state.Version++
	}
}

func (m *ClusterManager) procTick() {
	err := m.updateState(func(state *jsonState) error {
		if !m.containsMyNodeEntry(state) {
			// I've been ejected from the cluster...
			if m.isRunning {
				m.isRunning = false
				m.handler.Disconnected()
			}
		}

		m.updateMyNodeEntry(state)
		m.deleteDeadNodes(state)

		return nil
	})

	if err != nil {
		m.numFailedTicks++
		if m.isRunning {
			// TODO: Use a time based algorithm here instead
			if m.numFailedTicks >= 3 {
				m.isRunning = false
				m.handler.Disconnected()
			}
		}
		return
	}
}

func (m *ClusterManager) procShutdown() {
	m.handler.Disconnected()

	m.updateState(func(state *jsonState) error {
		m.removeMyNodeEntry(state)
		return nil
	})
}

func (m *ClusterManager) threadProc() {
	nextTick := time.Now().Add(m.state.TickPeriod)

	for {
		select {
		case <- time.After(nextTick.Sub(time.Now())):
			m.procTick()
			nextTick = time.Now().Add(m.state.TickPeriod)
		case event, ok := <- m.eventCh:
			if !ok {
				m.procShutdown()
				return
			}

			switch event := event.(type) {
			case *updateDataEvent:
				event.waitCh <- m.procUpdateData(event.mutateFn)
			default:
			}
		}
	}
}

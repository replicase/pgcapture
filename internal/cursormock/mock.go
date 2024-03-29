// Code generated by MockGen. DO NOT EDIT.
// Source: ../../pkg/cursor/tracker.go

// Package cursormock is a generated GoMock package.
package cursormock

import (
	reflect "reflect"

	pulsar "github.com/apache/pulsar-client-go/pulsar"
	gomock "github.com/golang/mock/gomock"
	cursor "github.com/replicase/pgcapture/pkg/cursor"
)

// MockTracker is a mock of Tracker interface.
type MockTracker struct {
	ctrl     *gomock.Controller
	recorder *MockTrackerMockRecorder
}

// MockTrackerMockRecorder is the mock recorder for MockTracker.
type MockTrackerMockRecorder struct {
	mock *MockTracker
}

// NewMockTracker creates a new mock instance.
func NewMockTracker(ctrl *gomock.Controller) *MockTracker {
	mock := &MockTracker{ctrl: ctrl}
	mock.recorder = &MockTrackerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTracker) EXPECT() *MockTrackerMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockTracker) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockTrackerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockTracker)(nil).Close))
}

// Commit mocks base method.
func (m *MockTracker) Commit(cp cursor.Checkpoint, mid pulsar.MessageID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", cp, mid)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockTrackerMockRecorder) Commit(cp, mid interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockTracker)(nil).Commit), cp, mid)
}

// Last mocks base method.
func (m *MockTracker) Last() (cursor.Checkpoint, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Last")
	ret0, _ := ret[0].(cursor.Checkpoint)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Last indicates an expected call of Last.
func (mr *MockTrackerMockRecorder) Last() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Last", reflect.TypeOf((*MockTracker)(nil).Last))
}

// Start mocks base method.
func (m *MockTracker) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockTrackerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockTracker)(nil).Start))
}

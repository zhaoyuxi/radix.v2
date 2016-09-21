package pubsub

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func testClients(t *T, timeout time.Duration) (*redis.Client, *SubClient) {
	pub, err := redis.DialTimeout("tcp", "localhost:6379", timeout)
	require.Nil(t, err)

	sub, err := redis.DialTimeout("tcp", "localhost:6379", timeout)
	require.Nil(t, err)

	return pub, NewSubClient(sub)
}

// Test that pubsub is still usable after a timeout
func TestTimeout(t *T) {
	go func() {
		time.Sleep(10 * time.Second)
		t.Fatal()
	}()

	pub, sub := testClients(t, 500*time.Millisecond)
	require.Nil(t, sub.Subscribe("timeoutTestChannel").Err)

	r := sub.Receive() // should timeout after a second
	assert.Equal(t, Error, r.Type)
	assert.NotNil(t, r.Err)
	assert.True(t, r.Timeout())

	waitCh := make(chan struct{})
	go func() {
		r = sub.Receive()
		close(waitCh)
	}()
	require.Nil(t, pub.Cmd("PUBLISH", "timeoutTestChannel", "foo").Err)
	<-waitCh

	assert.Equal(t, Message, r.Type)
	assert.Equal(t, "timeoutTestChannel", r.Channel)
	assert.Equal(t, "foo", r.Message)
	assert.Nil(t, r.Err, "%s", r.Err)
	assert.False(t, r.Timeout())
}

func TestSubscribe(t *T) {
	pub, sub := testClients(t, 10*time.Second)

	channel := randStr()
	message := randStr()

	sr := sub.Subscribe(channel)
	require.Nil(t, sr.Err)
	assert.Equal(t, Subscribe, sr.Type)
	assert.Equal(t, 1, sr.SubCount)

	subChan := make(chan *SubResp)
	go func() { subChan <- sub.Receive() }()

	require.Nil(t, pub.Cmd("PUBLISH", channel, message).Err)

	select {
	case sr = <-subChan:
	case <-time.After(10 * time.Second):
		t.Fatal("Took too long to Receive message")
	}

	require.Nil(t, sr.Err)
	assert.Equal(t, Message, sr.Type)
	assert.Equal(t, message, sr.Message)

	sr = sub.Unsubscribe(channel)
	require.Nil(t, sr.Err)
	assert.Equal(t, Unsubscribe, sr.Type)
	assert.Equal(t, 0, sr.SubCount)
}

func TestPSubscribe(t *T) {
	pub, sub := testClients(t, 10*time.Second)

	pattern := randStr() + "*"
	message := randStr()

	sr := sub.PSubscribe(pattern)
	require.Nil(t, sr.Err)
	assert.Equal(t, Subscribe, sr.Type)
	assert.Equal(t, 1, sr.SubCount)

	subChan := make(chan *SubResp)
	go func() { subChan <- sub.Receive() }()

	r := pub.Cmd("PUBLISH", pattern+"_"+randStr(), message)
	require.Nil(t, r.Err)

	select {
	case sr = <-subChan:
	case <-time.After(10 * time.Second):
		t.Fatal("Took too long to Receive message")
	}

	require.Nil(t, sr.Err)
	assert.Equal(t, Message, sr.Type)
	assert.Equal(t, pattern, sr.Pattern)
	assert.Equal(t, message, sr.Message)

	sr = sub.PUnsubscribe(pattern)
	require.Nil(t, sr.Err)
	assert.Equal(t, Unsubscribe, sr.Type)
	assert.Equal(t, 0, sr.SubCount)
}

package diseven

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func mustNewDisEvent(numberNode uint16) *DisEvent {
	return InitDisEvent(numberNode)
}

func TestGetNumberMasterNode(t *testing.T) {
	d := mustNewDisEvent(10)
	assert.Equal(t, uint16(10), d.GetNumberMasterNode())
}

func TestGenKeyWithHashTag(t *testing.T) {
	d := mustNewDisEvent(4)
	assert.Equal(t, "{Hash:3}.key", d.GenKeyWithHashTag("key"))
}

func TestDisEvent_GenMultiKeyWithHashTag(t *testing.T) {
	d := mustNewDisEvent(4)
	result := d.GenMultiKeyWithHashTag([]string{"key", "key1", "key2", "key3"})
	fmt.Println(result)
}

//result [{Hash:3}.key {Hash:1}.key1 {Hash:2}.key2 {Hash:3}.key3]

func TestDisEvent_GetMultiKeyQuery(t *testing.T) {
	d := mustNewDisEvent(4)
	result := d.GetMultiKeyQuery([]string{"key", "key1", "key2", "key3"})
	fmt.Println(result)
}
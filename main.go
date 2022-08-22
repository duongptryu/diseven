package diseven

import "fmt"

type DisEvent struct {
	NumberMasterNode uint16
	Prefix           string
}

var mapNodeNumberToPrefix = map[uint16]string{1: "Hash", 2: "Hash", 3: "Hash", 4: "Hash"}

func InitDisEvent(numberNode uint16) *DisEvent {
	var randomPrefix = "prefix"
	if prefix, exist := mapNodeNumberToPrefix[numberNode]; exist {
		return &DisEvent{
			NumberMasterNode: numberNode,
			Prefix:          prefix,
		}
	}
	return &DisEvent{
		NumberMasterNode: numberNode,
		Prefix:          randomPrefix,
	}
}

func (d *DisEvent) GetNumberMasterNode() uint16 {
	return d.NumberMasterNode
}

func (d *DisEvent) SetNumberMasterNode(numberNode uint16) {
	d.NumberMasterNode = numberNode
}

func (d *DisEvent) GetPrefix() string {
	return d.Prefix
}

func (d *DisEvent) SetPrefix(prefix string) {
	d.Prefix = prefix
}

func (d *DisEvent) GenKeyWithHashTag(key string) string {
	dividend := Crc16sum(key) % d.NumberMasterNode
	return fmt.Sprintf("{%s:%d}.%s", d.Prefix, dividend, key)
}

func (d *DisEvent) GenMultiKeyWithHashTag(keys []string) []string {
	var result = make([]string, len(keys))
	for i := range keys {
		dividend := Crc16sum(keys[i]) % d.NumberMasterNode
		result[i] = fmt.Sprintf("{%s:%d}.%s", d.Prefix, dividend, keys[i])
	}
	return result
}

func (d *DisEvent) GetMultiKeyQuery(keys []string) map[int8][]string {
	var result = make(map[int8][]string)
	for i := range keys {
		dividend := int8(Crc16sum(keys[i]) % d.NumberMasterNode)
		result[dividend] = append(result[dividend], fmt.Sprintf("{%s:%d}.%s", d.Prefix, dividend, keys[i]))
	}
	return result
}

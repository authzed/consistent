package hashring

import "strings"

type nodeRecord struct {
	hashvalue    uint64
	nodeKey      string
	member       Member
	virtualNodes []virtualNode
}

type virtualNode struct {
	hashvalue uint64
	members   nodeRecord
}

func less(a, b virtualNode) bool {
	if a.hashvalue == b.hashvalue {
		if a.members.hashvalue == b.members.hashvalue {
			return strings.Compare(a.members.nodeKey, b.members.nodeKey) < 0
		}
		return a.members.hashvalue < b.members.hashvalue
	}
	return a.hashvalue < b.hashvalue
}

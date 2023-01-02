package sorters

import (
	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
	"sort"
)

type conditionsComparator struct {
	knownConditions []kubernetes.SuppliedCondition
}

func NewConditionComparator(conditions []kubernetes.SuppliedCondition) func(n1, n2 *v1.Node) bool {
	cc := &conditionsComparator{knownConditions: conditions}
	return cc.CompareNodeConditionsPriorities
}

func (cc *conditionsComparator) CompareNodeConditionsPriorities(n1, n2 *v1.Node) bool {
	return cc.compareConditionsPriorities(
		kubernetes.GetNodeOffendingConditions(n1, cc.knownConditions),
		kubernetes.GetNodeOffendingConditions(n2, cc.knownConditions))

}

func (cc *conditionsComparator) compareConditionsPriorities(c1, c2 []kubernetes.SuppliedCondition) bool {
	sort.Sort(byHighPriority(c1))
	sort.Sort(byHighPriority(c2))

	for i := 0; ; i++ {
		if len(c1) <= i {
			return false // whatever is in c2, c1 can't take priority
		}
		if len(c2) <= i {
			return true // whatever is in c1, it takes priority
		}
		if c1[i].Priority != c2[i].Priority {
			return c1[i].Priority > c2[i].Priority
		}
	}
}

// ByAge implements sort.Interface for []Person based on
// the Age field.
type byHighPriority []kubernetes.SuppliedCondition

func (a byHighPriority) Len() int           { return len(a) }
func (a byHighPriority) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byHighPriority) Less(i, j int) bool { return a[i].Priority > a[j].Priority } // the func is called less for the sort interface, but we return the higher priority first!

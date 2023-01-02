package sorters

import (
	"github.com/planetlabs/draino/internal/kubernetes"
	"testing"
)

func Test_conditionsComparator_compareConditionsPriorities(t *testing.T) {
	prio0 := kubernetes.SuppliedCondition{Type: "prio0", Priority: 0}
	prio10 := kubernetes.SuppliedCondition{Type: "prio10", Priority: 10}
	prio1000 := kubernetes.SuppliedCondition{Type: "prio1000", Priority: 1000}
	knownConditions := []kubernetes.SuppliedCondition{
		prio0, prio1000, prio10,
	}
	tests := []struct {
		name string
		c1   []kubernetes.SuppliedCondition
		c2   []kubernetes.SuppliedCondition

		want bool
	}{
		{
			name: "nothing",
			c1:   nil,
			c2:   nil,
			want: false,
		},
		{
			name: "only c1 has a condition",
			c1:   []kubernetes.SuppliedCondition{prio0},
			c2:   nil,
			want: true,
		},
		{
			name: "only c2 has a condition",
			c1:   nil,
			c2:   []kubernetes.SuppliedCondition{prio10},
			want: false,
		},
		{
			name: "same condition",
			c1:   []kubernetes.SuppliedCondition{prio0},
			c2:   []kubernetes.SuppliedCondition{prio0},
			want: false,
		},
		{
			name: "more in c1",
			c1:   []kubernetes.SuppliedCondition{prio10, prio0},
			c2:   []kubernetes.SuppliedCondition{prio10},
			want: true,
		},
		{
			name: "more in c2",
			c1:   []kubernetes.SuppliedCondition{prio10},
			c2:   []kubernetes.SuppliedCondition{prio10, prio0},
			want: false,
		},
		{
			name: "only one in c1 but higher",
			c1:   []kubernetes.SuppliedCondition{prio1000},
			c2:   []kubernetes.SuppliedCondition{prio10, prio0},
			want: true,
		},
		{
			name: "only one in c2 but higher",
			c1:   []kubernetes.SuppliedCondition{prio10, prio0},
			c2:   []kubernetes.SuppliedCondition{prio1000},
			want: false,
		},
		{
			name: "same count c1 wins",
			c1:   []kubernetes.SuppliedCondition{prio10, prio1000},
			c2:   []kubernetes.SuppliedCondition{prio1000, prio0},
			want: true,
		},
		{
			name: "same count c2 wins",
			c1:   []kubernetes.SuppliedCondition{prio0, prio1000},
			c2:   []kubernetes.SuppliedCondition{prio1000, prio10},
			want: false,
		},
		{
			name: "same count no win",
			c1:   []kubernetes.SuppliedCondition{prio0, prio1000},
			c2:   []kubernetes.SuppliedCondition{prio1000, prio0},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := &conditionsComparator{
				knownConditions: knownConditions,
			}
			if got := cc.compareConditionsPriorities(tt.c1, tt.c2); got != tt.want {
				t.Errorf("CompareNodeConditionsPriorities() = %v, want %v", got, tt.want)
			}
		})
	}
}

package utils

import "sync"

type DataMap struct {
	sync.RWMutex
	M map[string]interface{} // must be public for serialization
}

func NewDataMap() *DataMap {
	return &DataMap{
		M: map[string]interface{}{},
	}
}

func (dm *DataMap) Set(k string, v interface{}) {
	dm.Lock()
	dm.M[k] = v
	dm.Unlock()
}

func (dm *DataMap) Get(k string) (interface{}, bool) {
	dm.RLock()
	v, ok := dm.M[k]
	dm.RUnlock()
	return v, ok
}

func (dm *DataMap) Inject(dm2 *DataMap) {
	dm2.RLock()
	dm.Lock()
	for k, v := range dm2.M {
		if len(k) > 0 && k[0] == '_' { // skip propagation of keys starting with "_" that are server side struct
			continue
		}
		dm.M[k] = v
	}
	dm.Unlock()
	dm2.RUnlock()
}

package sinks

import (
	"container/ring"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

/*

TODO
* DONE ReverseDo
* NOPE Pre-allocate TimeStampedValue
* DONE Keep track of count/sum/sum^2 on each ingest/filter
* NOPE filter on each ingest
* DONE smart filter/short circuit using ReverseDo
* DONE do/rdo fn return bool

*/

type WindowSink struct {
	sync.RWMutex
	maxValues int
	maxAge    time.Duration

	gauges  map[string]*TimeStampedValue
	samples map[string]*ValueRing
}

func NewWindowSink(maxAge time.Duration, maxValues int) *WindowSink {
	ws := &WindowSink{
		maxAge:    maxAge,
		maxValues: maxValues,
		samples:   make(map[string]*ValueRing),
	}
	return ws
}

var (
	errNoValue      = errors.New("No value set")
	errExpiredValue = errors.New("Value has expired")
)

type ValueRing struct {
	sync.RWMutex
	r         *ring.Ring
	maxValues int
	maxAge    time.Duration
	count     int
	sum       float64
	sumSq     float64
}

func NewValueRing(maxValues int, maxAge time.Duration) *ValueRing {
	vr := &ValueRing{
		r:         ring.New(maxValues),
		maxValues: maxValues,
		maxAge:    maxAge,
	}
	return vr
}

type TimeStampedValue struct {
	Recorded time.Time
	Value    float32
}

func newTimeStampedValue(v float32) *TimeStampedValue {
	return &TimeStampedValue{
		Recorded: time.Now(),
		Value:    v,
	}
}

func (tsv *TimeStampedValue) String() string {
	return fmt.Sprintf("<Recorded:%s Value:%f>", tsv.Recorded, tsv.Value)
}

func (ws *WindowSink) Sample(key []string) *ValueRing {
	ws.RLock()
	defer ws.RUnlock()

	k := ws.flattenKey(key)
	s, ok := ws.samples[k]
	if !ok {
		return nil
	}
	s.filter()
	return s
}

func (ws *WindowSink) SetGauge(key []string, val float32) {
	ws.Lock()
	defer ws.Unlock()
}

// Should emit a Key/Value pair for each call
func (ws *WindowSink) EmitKey(key []string, val float32) {
	ws.Lock()
	defer ws.Unlock()
}

// Counters should accumulate values
func (ws *WindowSink) IncrCounter(key []string, val float32) {
	ws.Lock()
	defer ws.Unlock()
}

// Samples are for timing information, where quantiles are used
func (ws *WindowSink) AddSample(key []string, val float32) {
	ws.Lock()
	defer ws.Unlock()

	k := ws.flattenKey(key)

	var vr *ValueRing
	var ok bool

	if vr, ok = ws.samples[k]; !ok {
		vr = NewValueRing(ws.maxValues, ws.maxAge)
		ws.samples[k] = vr
	}

	vr.ingest(val)
	//vr.filter()
}

// Flattens the key for formatting, removes spaces
func (ws *WindowSink) flattenKey(parts []string) string {
	joined := strings.Join(parts, ".")
	return strings.Replace(joined, " ", "_", -1)
}

func (vr *ValueRing) ingest(val float32) {
	vr.Lock()
	defer vr.Unlock()

	vr.r = vr.r.Next()
	if vr.r.Value != nil {
		tsv := vr.r.Value.(*TimeStampedValue)
		vr.decrementTSV(tsv)
	}
	vr.r.Value = newTimeStampedValue(val)
	vr.count++
	if vr.count > vr.maxValues {
		vr.count = vr.maxValues
	}

	vr.sum += float64(val)
	vr.sumSq += float64(val) * float64(val)
}

func castAndCheck(now time.Time, maxAge time.Duration, r *ring.Ring) (*TimeStampedValue, error) {
	if r.Value == nil {
		return nil, errNoValue
	}

	tsv := r.Value.(*TimeStampedValue)

	if now.Sub(tsv.Recorded) > maxAge {
		return tsv, errExpiredValue
	}

	return tsv, nil
}

func (vr *ValueRing) filter() {
	vr.Lock()
	defer vr.Unlock()

	now := time.Now()

	f := func(r *ring.Ring) bool {
		if r.Value != nil {
			tsv, err := castAndCheck(now, vr.maxAge, r)
			if err == errExpiredValue {
				vr.decrementTSV(tsv)
				r.Value = nil
				vr.count--
			}
			return true
		}
		return false
	}

	vr.rdo(f)
}

func (vr *ValueRing) decrementTSV(tsv *TimeStampedValue) {
	vr.sum -= float64(tsv.Value)
	vr.sumSq -= float64(tsv.Value) * float64(tsv.Value)
}

func (vr *ValueRing) Count() int {
	vr.RLock()
	defer vr.RUnlock()
	return vr.count
}

func (vr *ValueRing) Sum() float64 {
	vr.RLock()
	defer vr.RUnlock()
	return vr.sum
}

func (vr *ValueRing) Stddev() float64 {
	vr.RLock()
	defer vr.RUnlock()

	num := (float64(vr.count) * vr.sumSq) - math.Pow(vr.sum, 2)
	div := float64(vr.count * (vr.count - 1))

	if div == 0 {
		return 0
	}

	return math.Sqrt(num / div)
}

func (vr *ValueRing) Mean() float32 {
	vr.RLock()
	defer vr.RUnlock()

	if vr.count == 0 {
		return 0
	}

	return float32(vr.sum / float64(vr.count))
}

func (vr *ValueRing) Min() float32 {
	vr.RLock()
	defer vr.RUnlock()

	if vr.count == 0 {
		return float32(0)
	}

	v := float32(math.MaxFloat32)

	vr.rdo(func(r *ring.Ring) bool {
		if r.Value != nil {
			tsv := r.Value.(*TimeStampedValue)
			if tsv.Value < v {
				v = tsv.Value
			}
			return true
		}
		return false
	})
	return v
}

func (vr *ValueRing) Max() float32 {
	vr.RLock()
	defer vr.RUnlock()

	if vr.count == 0 {
		return float32(0)
	}

	v := -float32(math.MaxFloat32)

	vr.rdo(func(r *ring.Ring) bool {
		if r.Value != nil {
			tsv := r.Value.(*TimeStampedValue)
			if tsv.Value > v {
				v = tsv.Value
			}
			return true
		}
		return false
	})
	return v
}

func (vr *ValueRing) ToSlice() []*TimeStampedValue {
	vr.RLock()
	defer vr.RUnlock()

	s := make([]*TimeStampedValue, vr.Count())
	i := 0

	vr.r.Do(func(v interface{}) {
		if v != nil {
			tsv := v.(*TimeStampedValue)
			s[i] = &TimeStampedValue{
				Recorded: tsv.Recorded,
				Value:    tsv.Value,
			}
			i++
		}
	})

	return s
}

type ringFn func(r *ring.Ring) bool

func (vr *ValueRing) do(f ringFn) {
	if vr.r != nil {
		for p := vr.r.Next(); p != vr.r; p = p.Next() {
			if !f(p) {
				return
			}
		}
		if !f(vr.r) {
			return
		}
	}
}

// revserse
func (vr *ValueRing) rdo(f ringFn) {
	if vr.r != nil {
		if !f(vr.r) {
			return
		}
		for p := vr.r.Prev(); p != vr.r; p = p.Prev() {
			if !f(p) {
				return
			}
		}
	}
}

package dfa

import (
	"bytes"
	"fmt"
)

type State string

func (s State) String() string {
	return string(s)
}

type Letter string

func (l Letter) String() string {
	return string(l)
}

type DFA struct {
	q      map[State]bool                     // States
	e      map[Letter]bool                    // Alphabet
	d      map[domainelement]*codomainelement // Transition
	q0     State                              // Start State
	f      map[State]bool                     // Terminal States
	done   chan laststate                     // Termination channel
	input  *Letter                            // Inputs to the DFA
	stop   chan struct{}                      // Stops the DFA
	logger func(State)                        // Logger for transitions
}

type domainelement struct {
	l Letter
	s State
}

type codomainelement struct {
	s    State
	exec interface{}
}

type laststate struct {
	s        State
	accepted bool
}

func New() *DFA {
	return &DFA{
		q:      make(map[State]bool),
		e:      make(map[Letter]bool),
		f:      make(map[State]bool),
		d:      make(map[domainelement]*codomainelement),
		done:   make(chan laststate, 1),
		stop:   make(chan struct{}),
		logger: func(State) {},
	}
}

// SetTransition, argument 'exec' must be a function that will supply the next letter if the
// 'to' state is non-terminal.
func (m *DFA) SetTransition(from State, input Letter, to State, exec interface{}) {
	if exec == nil {
		panic("stateful computation cannot be nil")
	}
	if from == State("") || to == State("") {
		panic("state cannot be defined as the empty string")
	}
	switch exec.(type) {
	case func():
		if !m.f[to] {
			panic(fmt.Sprintf("stateful computation must be of type 'func() Letter' for non-terminal '%v' state", to))
		}
	case func() Letter:
		if m.f[to] {
			panic(fmt.Sprintf("stateful computation must be of type 'func()' for terminal '%v' state", to))
		}
	default:
		panic("stateful computation must be of type 'func()' or 'func() Letter")
	}
	m.q[to] = true
	m.q[from] = true
	m.e[input] = true
	de := domainelement{l: input, s: from}
	if _, ok := m.d[de]; !ok {
		m.d[de] = &codomainelement{s: to, exec: exec}
	}
}

// SetStartState, there can be only one.
func (m *DFA) SetStartState(q0 State) {
	m.q0 = q0
}

// SetTerminalStates, there can be more than one. Once entered the
// DFA will stop.
func (m *DFA) SetTerminalStates(f ...State) {
	for _, q := range f {
		m.f[q] = true
	}
}

func (m *DFA) SetTransitionLogger(logger func(State)) {
	m.logger = logger
}

// States of the DFA.
func (m *DFA) States() []State {
	q := make([]State, 0, len(m.q))
	for s, _ := range m.q {
		q = append(q, s)
	}
	return q
}

// Alphabet of the DFA.
func (m *DFA) Alphabet() []Letter {
	e := make([]Letter, 0, len(m.e))
	for l, _ := range m.e {
		e = append(e, l)
	}
	return e
}

// Run the DFA, blocking until Stop is called or the DFA enters a terminal state.
// Returns the last state and true if the last state was a terminal state.
func (m *DFA) Run(init interface{}) (State, bool) {
	// Check some pre-conditions.
	if init == nil {
		panic("initial stateful computation is nil")
	}
	if m.q0 == State("") {
		panic("no start state definied")
	}
	if len(m.f) == 0 {
		panic("no terminal states definied")
	}
	if _, ok := m.q[m.q0]; !ok {
		panic(fmt.Sprintf("start state '%v' is not in the set of states", m.q0))
	}
	for s, _ := range m.f {
		if _, ok := m.q[s]; !ok {
			panic(fmt.Sprintf("terminal state '%v' is not in the set of states", s))
		}
	}
	// Run the DFA.
	go func() {
		defer close(m.done)
		// The current state, starts at q0.
		s := m.q0
		// Run the initial stateful computation.
		if m.f[s] {
			// If the state is a terminal state then the DFA has
			// accepted the input sequence and it can stop.
			m.done <- laststate{s, true}
			return
		} else {
			// Otherwise continue reading generated input
			// by starting the next stateful computation.
			switch init := init.(type) {
			case func():
				m.logger(s)
				init()
			case func() Letter:
				m.logger(s)
				l := init()
				m.input = &l
			}
		}
		for {
			var stopnow bool
			select {
			case <-m.stop:
				stopnow = true
			default:
			}
			if stopnow {
				break
			}
			if m.input != nil {
				l := *m.input
				// Reject upfront if letter is not in alphabet.
				if !m.e[l] {
					panic(fmt.Sprintf("letter '%v' is not in alphabet", l))
				}
				// Compose the domain element, so that the co-domain
				// element can be found via the transition function.
				de := domainelement{l: l, s: s}
				// Check the transition function.
				if coe := m.d[de]; coe != nil {
					s = coe.s
					switch exec := coe.exec.(type) {
					case func():
						m.logger(s)
						exec()
					case func() Letter:
						m.logger(s)
						l := exec()
						m.input = &l
					}
					if m.f[s] {
						// If the new state is a terminal state then
						// the DFA has accepted the input sequence
						// and it can stop.
						m.done <- laststate{s, true}
						return
					}
				} else {
					// Otherwise stop the DFA with a rejected state,
					// the DFA has rejected the input sequence.
					panic(fmt.Sprintf("no state transition for input '%v' from '%v'", l, s))
				}
			}
		}
		// The caller has closed the input channel, check if the
		// current state is accepted or rejected by the DFA.
		if m.f[s] {
			m.done <- laststate{s, true}
		} else {
			m.done <- laststate{s, false}
		}
	}()
	return m.result()
}

// Stop the DFA.
func (m *DFA) Stop() {
	close(m.stop)
}

// GraphViz representation string which can be copy-n-pasted into
// any online tool like http://graphs.grevian.org/graph to get
// a diagram of the DFA.
func (m *DFA) GraphViz() string {
	var buf bytes.Buffer
	buf.WriteString("digraph {\n")
	for do, cdo := range m.d {
		if do.s == m.q0 {
			buf.WriteString(fmt.Sprintf("    \"%s\" -> \"%s\"[label=\"%s\"];\n", do.s, cdo.s, do.l))
		} else if m.f[cdo.s] {
			buf.WriteString(fmt.Sprintf("    \"%s\" -> \"%s\"[label=\"%s\"];\n", do.s, cdo.s, do.l))
		} else {
			buf.WriteString(fmt.Sprintf("    \"%s\" -> \"%s\"[label=\"%s\"];\n", do.s, cdo.s, do.l))
		}
	}
	buf.WriteString("}")
	return buf.String()
}

func (m *DFA) result() (State, bool) {
	t := <-m.done
	return t.s, t.accepted
}

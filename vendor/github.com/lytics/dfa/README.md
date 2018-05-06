dfa
===

[![Build Status](https://travis-ci.org/lytics/dfa.svg?branch=master)](https://travis-ci.org/lytics/dfa)
[![GoDoc](https://godoc.org/github.com/lytics/dfa?status.svg)](https://godoc.org/github.com/lytics/dfa)

The `dfa` package implements a deterministic finite automata for use to define stateful
computations that are easier understood when transitions are specified explicitly. The
API is more interested in using the DFA to clearly define stateful computation, rather
than actually being used to recognize languages.

The implementation has all the normal compnents:

 * a finite set of states (Q)
 * a finite set of input symbols called the alphabet (Σ)
 * a transition function (δ : Q × Σ → Q)
 * a start state (q0 ∈ Q)
 * a set of terminal states (F ⊆ Q)

For each transition, Q × Σ → Q, you must specify a function to run that will supply the
next letter, rather than the input being on some "tape."

### Importing

    import "github.com/lytics/dfa"

### Quick Example

```go
    // Define states.
    Starting = dfa.State("starting")
    Finishing = dfa.State("finishing")

    // Define letters.
    Done = dfa.Letter("done")
    Repeat = dfa.Letter("repeat")

    // Error reporting is done by user code.
    var errors []error

    // Define stateful computations.
    starting := func() dfa.Letter {
        if err := do(); err != nil {
            errors = append(errors, err)
            return Repeat
        } else {
            return Done
        }
    }
    finishing := func() {
        fmt.Println("all finished")
    }

    // Order matter, set start and terminal states first.
    d := dfa.New()
    d.SetStartState(Starting)
    d.SetTerminalStates(Finishing)
    d.SetTransition(Starting, Done, Finishing, finishing)
    d.SetTransition(Starting, Repeat, Starting, starting)

    // Calls the given function in a new go-routine,
    // unless there were initialization errors.
    // Blocks until the DFA accepts its input or
    // its Stop() function is called. The final state
    // is returned, and accepted == true if the final
    // state is a terminal state.
    final, accepted := d.Run(starting)

    // Error handling is up to the user application.
    for _, err := range errors {
        ...
    }
```

### Stateful Computations

The functions given when defining the transitions must have one of
two types:

 * `func()`
 * `func() dfa.Letter`

Functions associated with a transition ending in a terminal state must
give a function that returns no letter. Functions that transition to
non-terminal states must return a letter.

### GraphViz

Calling the `GraphViz` method will generate a string of [GraphViz](http://graphs.grevian.org/graph)
text which can then generate a flow diagram of the DFA.

```go
    // States
    Starting := State("starting")
    Running := State("running")
    Resending := State("resending")
    Finishing := State("finishing")
    Exiting := State("exiting")
    Terminating := State("terminating")
    // Letters
    Failure := Letter("failure")
    SendFailure := Letter("send-failure")
    SendSuccess := Letter("send-success")
    EverybodyStarted := Letter("everybody-started")
    EverybodyFinished := Letter("everybody-finished")
    ProducersFinished := Letter("producers-finished")
    Exit := Letter("exit")

    d := New()
    d.SetStartState(Starting)
    d.SetTerminalStates(Exiting, Terminating)

    ...

    d.SetTransition(Starting, EverybodyStarted, Running, ...)
    d.SetTransition(Starting, Failure, Exiting, ...)
    d.SetTransition(Starting, Exit, Exiting, ...)

    d.SetTransition(Running, SendFailure, Resending, ...)
    d.SetTransition(Running, ProducersFinished, Finishing, ...)
    d.SetTransition(Running, Failure, Exiting, ...)
    d.SetTransition(Running, Exit, Exiting, ...)

    d.SetTransition(Resending, SendSuccess, Running, ...)
    d.SetTransition(Resending, SendFailure, Resending, ...)
    d.SetTransition(Resending, Failure, Exiting, ...)
    d.SetTransition(Resending, Exit, Exiting, ...)

    d.SetTransition(Finishing, EverybodyFinished, Terminating, ...)
    d.SetTransition(Finishing, Failure, Exiting, ...)
    d.SetTransition(Finishing, Exit, Exiting, ...)

    fmt.Println(d.GraphViz())
```

The above code generates the output:

```
digraph {
    "starting" -> "running"[label="everybody-started"];
    "resending" -> "exiting"[label="exit"];
    "starting" -> "exiting"[label="exit"];
    "running" -> "exiting"[label="failure"];
    "resending" -> "exiting"[label="failure"];
    "finishing" -> "exiting"[label="failure"];
    "starting" -> "exiting"[label="failure"];
    "running" -> "resending"[label="send-failure"];
    "running" -> "finishing"[label="producers-finished"];
    "running" -> "exiting"[label="exit"];
    "resending" -> "running"[label="send-success"];
    "resending" -> "resending"[label="send-failure"];
    "finishing" -> "terminating"[label="everybody-finished"];
    "finishing" -> "exiting"[label="exit"];
}
```

Which when given to the [GraphViz](http://graphs.grevian.org/graph) command line tool
would visualize the DFA like this:

![DFA](/dfa.png)
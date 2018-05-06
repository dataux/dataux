/*
The dfa package implements a deterministic finite automata to define stateful computations
that are easier understood when transitions are specified explicitly. The API is more
interested in using the DFA to clearly define stateful computation, rather than actually
being used to recognize languages.

Importing

    import "github.com/lytics/dfa"

Example

    Starting = dfa.State("starting")
    Finishing = dfa.State("finishing")
    
    Done = dfa.Letter("done")
    Repeat = dfa.Letter("repeat")
    
    var errors []error
    
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
    
    d := dfa.New()
    d.SetStartState(Starting)
    d.SetTerminalStates(Finishing)
    d.SetTransition(Starting, Done, Finishing, finishing)
    d.SetTransition(Starting, Repeat, Starting, starting)
    
    final, accepted := d.Run(starting)
    ...
    
    for _, err := range errors {
        ...
    }
*/
package dfa

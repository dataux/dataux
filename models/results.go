package models

type ResultWriter interface {
	WriteResult(Result) error
}

type Result interface{}

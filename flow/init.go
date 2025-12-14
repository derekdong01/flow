package flow

import (
	"fmt"
	"log"
	"time"
)

var GlobalOperators map[string]OperatorBuilder

func RegisterOperator(name string, builder OperatorBuilder) {
	if _, ok := GlobalOperators[name]; ok {
		panic(fmt.Sprintf("operator %s has been registered", name))
	}
	GlobalOperators[name] = builder
}

func init() {
	GlobalOperators = map[string]OperatorBuilder{}

	RegisterOperator(DefaultRoot, func() *Operator {
		obj := new(DefaultRootOperator)
		op := new(Operator)
		op.Name = DefaultRoot
		op.ObjOperator = obj
		return op
	})
}

const (
	DefaultRoot = "default_root"
)

type DefaultRootOperator struct {
	*Operator
}

func (d *DefaultRootOperator) DoImpl() error {
	log.Println("this is default_root")
	time.Sleep(time.Millisecond * 50)
	return nil
}

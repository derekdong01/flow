package main

import (
	"context"
	"fmt"
	"log"

	"github.com/derekdong01/go-flow/flow"
)

func main() {
	fmt.Println("Hello, World!")
	ctx := context.Background()

	builder := flow.NewFlowBuilder()
	// inst, err := builder.BuildFlow(ctx, "subflow.json")
	inst, err := builder.BuildFlow(ctx, "demo_complex.json")
	if err != nil {
		log.Fatal(err)
	}
	err = inst.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	_ = inst.Graphviz("./demo_complex.svg")
}

package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/derekdong01/golibrary/math"
)

const SPLIT = "_"

type Builder struct {
	confReader ConfReader
	confCacher ConfCacher
}

func NewFlowBuilder() *Builder {
	builder := Builder{
		confReader: NewConfReaderLocal(),
		confCacher: NewConfCacherLocal(),
	}
	return &builder
}

type (
	FlowConf struct {
		Name         string                   `json:"name"`
		Desc         string                   `json:"desc"`
		RootOperator string                   `json:"root"`
		Operators    []*OperatorConf          `json:"flow"`
		operatorMap  map[string]*OperatorConf `json:"-"`
	}

	OperatorConf struct {
		NodeName    string         `json:"nodeName"`
		Desc        string         `json:"desc"`
		OpName      string         `json:"opName"`
		HyperParams map[string]any `json:"hyperParams"`
		Param       []string       `json:"param"`
		Output      string         `json:"output"`
		Next        []string       `json:"next"`
		IsSubFlow   bool           `json:"isSubFlow"`
		SubFlowConf string         `json:"subFlowConf"`
	}
)

func (b *Builder) BuildFlow(ctx context.Context, flowName string) (*Flow, error) {
	flowConf := b.MustLoadFlowConf(flowName)
	flow := new(Flow)
	flow.name = flowConf.Name
	flow.SetContext(ctx)

	uniqueOperators := make(map[string]*Operator, len(flowConf.operatorMap))
	flowDepth := 0
	// 创建其他节点
	for _, operatorConf := range flowConf.Operators {
		from := operatorConf.NodeName
		if _, ok := uniqueOperators[from]; !ok {
			op, err := b.CreateOperator(ctx, operatorConf)
			if err != nil {
				return nil, err
			}
			op.SetFlow(flow)
			uniqueOperators[from] = op
		}
		for _, nextName := range operatorConf.Next {
			nextOperConf, ok := flowConf.operatorMap[nextName]
			if !ok {
				return nil, fmt.Errorf("operator conf %s required", nextName)
			}
			to := nextOperConf.NodeName
			if _, ok := uniqueOperators[to]; !ok {
				op, err := b.CreateOperator(ctx, nextOperConf)
				if err != nil {
					return nil, err
				}
				op.SetFlow(flow)
				uniqueOperators[to] = op
			}
			uniqueOperators[to].maxDepth = math.Max(uniqueOperators[from].maxDepth+1, uniqueOperators[to].maxDepth)
			flowDepth = math.Max(flowDepth, uniqueOperators[to].maxDepth)
			uniqueOperators[from].NextList = append(uniqueOperators[from].NextList, uniqueOperators[to])
			uniqueOperators[to].PriorList = append(uniqueOperators[to].PriorList, uniqueOperators[from])
		}
	}
	flow.depth = flowDepth
	flow.SetStart(uniqueOperators[flowConf.RootOperator])

	if err := flow.GraphDiagnose(flow.start); err != nil {
		return nil, err
	}

	// 判断最后一个算子是否只有一个
	cnt := 0
	for _, op := range uniqueOperators {
		if op.maxDepth == flowDepth {
			op.isLast = true
			flow.SetEnd(op)
			cnt++
		}
	}
	if cnt != 1 {
		return nil, errors.New("more than one end operator")
	}

	return flow, nil
}

func (b *Builder) CreateOperator(ctx context.Context, operatorConf *OperatorConf) (*Operator, error) {
	if operatorConf.IsSubFlow {
		subFlow, err := b.BuildFlow(ctx, operatorConf.SubFlowConf)
		if err != nil {
			return nil, err
		}
		subFlow.name = operatorConf.NodeName
		subFlow.isSubFlow = true
		subFlow.SetContext(ctx)
		op := Operator{
			Name:         operatorConf.NodeName,
			OpName:       operatorConf.SubFlowConf,
			ObjOperator:  subFlow,
			OperatorConf: operatorConf,
		}
		subFlow.Operator = &op
		return &op, nil
	}
	opFunc, ok := GlobalOperators[operatorConf.OpName]
	if !ok {
		panic(fmt.Sprintf("func is not defined:%s", operatorConf.OpName))
	}
	op := opFunc()
	op.OperatorConf = operatorConf
	op.Name = operatorConf.NodeName
	op.OpName = operatorConf.OpName
	return op, nil
}

func (b *Builder) MustLoadFlowConf(name string) *FlowConf {
	var confBytes []byte
	if b.confCacher != nil {
		val, ok := b.confCacher.Get(name)
		if ok {
			confBytes = val.([]byte)
		}
	}
	if len(confBytes) == 0 {
		bytes, err := b.confReader.Read(name)
		if err != nil {
			panic(fmt.Sprintf("read flow conf failed: %v", err))
		}
		confBytes = bytes
		if b.confCacher != nil {
			b.confCacher.SetDefault(name, confBytes)
		}
	}
	var flowConf FlowConf
	err := json.Unmarshal(confBytes, &flowConf)
	if err != nil {
		panic(fmt.Sprintf("json unmarshal flow conf failed: %v", err))
	}

	if len(flowConf.Name) == 0 {
		panic("flow name must required")
	}

	if len(flowConf.RootOperator) == 0 {
		panic("root operator name must required")
	}

	if flowConf.operatorMap == nil {
		flowConf.operatorMap = make(map[string]*OperatorConf, len(flowConf.Operators))
	}
	for _, operatorConf := range flowConf.Operators {
		flowConf.operatorMap[operatorConf.NodeName] = operatorConf
	}

	if _, ok := flowConf.operatorMap[flowConf.RootOperator]; !ok {
		panic("root operator is not defined")
	}

	for _, operatorConf := range flowConf.Operators {
		if len(operatorConf.Next) > 0 {
			for _, nextName := range operatorConf.Next {
				if _, ok := flowConf.operatorMap[nextName]; !ok {
					panic(fmt.Sprintf("next operator %s is not defined", nextName))
				}
			}
		}
	}
	return &flowConf
}

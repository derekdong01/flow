package flow

type IOperator interface {
	DoImpl() error
}

type OperatorBuilder func() *Operator

type Operator struct {
	Name         string
	OpName       string
	ObjOperator  IOperator
	PriorList    []*Operator
	NextList     []*Operator
	OperatorConf *OperatorConf
	flow         *Flow // 算子所在flow
	maxDepth     int   //  算子在图中的最大深度
	isLast       bool  // 算子是否为图中最后一个算子
	ret          any   // 算子执行结果
}

func (o *Operator) SetFlow(flow *Flow) {
	o.flow = flow
}

func (o *Operator) GetFlow() *Flow {
	return o.flow
}

func (o *Operator) IsLast() bool {
	return o.isLast
}

func (o *Operator) SetRet(ret any) {
	o.ret = ret
}

func (o *Operator) GetRet() any {
	return o.ret
}

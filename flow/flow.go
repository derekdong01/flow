package flow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/derekdong01/golibrary/dag"
	"github.com/goccy/go-graphviz"
)

type Flow struct {
	*Operator

	name      string // flow name
	depth     int
	isSubFlow bool
	ctx       context.Context

	start *Operator // 起始算子
	end   *Operator // 结束算子

	// 数据存储
	kvs     map[string]any
	lockKvs sync.RWMutex
}

func (flow *Flow) DoImpl() error {
	if flow.isSubFlow {
		flow.SetContext(flow.Operator.GetFlow().GetContext())
	}
	if err := flow.Run(flow.ctx); err != nil {
		return err
	}

	// 如果是子图，将最后一个算子的输出存储到全局flow kv
	if flow.isSubFlow {
		lastOp := flow.GetEnd()
		if outputKey := lastOp.OperatorConf.Output; len(outputKey) > 0 {
			val := flow.GetKV(outputKey)
			flow.flow.SetGlobalKV(outputKey, val)
		}
	}
	return nil
}

func (flow *Flow) Run(ctx context.Context) error {
	if flow.start == nil {
		return errors.New("start is nil")
	}
	flow.SetContext(ctx)
	pipeline := dag.NewDAG()
	tmp := pipeline.Spawn(0)
	visited := make(map[*Operator]struct{})
	var queue = make([]*Operator, 0)
	queue = append(queue, flow.start)
	depth := 0
	for len(queue) > 0 {
		size := len(queue)
		depth++
		tasks := make([]dag.Task, 0, size)
		for i := 0; i < size; i++ {
			op := queue[i]
			tasks = append(tasks, func() error {
				cur := time.Now().Local()
				log.Printf("operator `%s` start at %s", op.Name, cur.Format(time.DateTime))
				defer func() {
					if err := recover(); err != nil {
						trace := make([]byte, 4096)
						runtime.Stack(trace[:], false)
						log.Printf("operator %s panic: %s", op.Name, string(trace))
					}
					log.Printf("operator `%s` end  cost[%d ms]", op.Name, time.Since(cur).Milliseconds())
				}()
				return op.ObjOperator.DoImpl()
			})
			visited[op] = struct{}{}
			for _, nextOp := range op.NextList {
				if _, ok := visited[nextOp]; ok || nextOp.maxDepth != depth {
					continue
				}
				queue = append(queue, nextOp)
				visited[nextOp] = struct{}{}
			}
		}
		tmp = tmp.Join().Spawn(0, tasks...)
		queue = queue[size:]
	}
	return pipeline.Run()
}

func (flow *Flow) GetFlowName() string {
	return flow.Name
}

func (flow *Flow) IsSubFlow() bool {
	return flow.isSubFlow
}

func (flow *Flow) SetContext(ctx context.Context) {
	flow.ctx = ctx
}

func (flow *Flow) GetContext() context.Context {
	return flow.ctx
}

func (flow *Flow) SetStart(start *Operator) {
	flow.start = start
}

func (flow *Flow) GetStart() *Operator {
	return flow.start
}

func (flow *Flow) SetEnd(end *Operator) {
	flow.end = end
}

func (flow *Flow) GetEnd() *Operator {
	return flow.end
}

func (flow *Flow) SetKV(key string, value any) {
	if len(key) == 0 {
		return
	}
	flow.lockKvs.Lock()
	defer flow.lockKvs.Unlock()

	if flow.kvs == nil {
		flow.kvs = make(map[string]any)
	}
	flow.kvs[key] = value
}

func (flow *Flow) SetGlobalKV(key string, value any) {
	flow.SetKV(key, value)
	if !flow.isSubFlow {
		return
	}
	key = flow.GetFlowName() + "-" + key
	flow.Operator.GetFlow().SetGlobalKV(key, value)
}

func (flow *Flow) GetKV(key string) any {
	flow.lockKvs.RLock()
	defer flow.lockKvs.RUnlock()
	// 优先从当前flow的kv中查找
	if v, ok := flow.kvs[key]; ok {
		return v
	}
	// 从全局flow的kv中查找
	if flow.isSubFlow {
		return flow.Operator.GetFlow().GetKV(key)
	}
	return nil
}

func (flow *Flow) GraphDiagnose(start *Operator) error {
	var dfs func(node *Operator, greyNodeMap, blackNodeMap, indegreeMap *map[*Operator]int) (bool, *Operator)

	dfs = func(node *Operator, greyNodeMap, blackNodeMap, indegreeMap *map[*Operator]int) (bool, *Operator) {
		if node == nil {
			return false, nil
		}
		blackNode := *blackNodeMap
		greyNode := *greyNodeMap
		_, ok := blackNode[node]
		if !ok {
			return false, nil
		}
		_, hasCircle := greyNode[node]
		if hasCircle {
			return true, node
		}
		greyNode[node] = 0
		for _, nextNode := range node.NextList {
			if nextNode != nil {
				hasCircle, errNode := dfs(nextNode, greyNodeMap, blackNodeMap, indegreeMap)
				if errNode != nil {
					return hasCircle, errNode
				}
			}
		}
		indegree := *indegreeMap
		indegree[node] = len(node.PriorList)
		blackNode[node] = 0
		return false, nil
	}
	greyNodeMap := make(map[*Operator]int, 0)
	blackNodeMap := make(map[*Operator]int, 0)
	indegreeMap := make(map[*Operator]int, 0)

	hasCircle, errNode := dfs(start, &greyNodeMap, &blackNodeMap, &indegreeMap)
	if hasCircle {
		return fmt.Errorf("flow has circle, errNode:%s", errNode.Name)
	}
	return nil
}

func (flow *Flow) Graphviz(svgFilePath string) error {
	ctx := context.Background()
	g, _ := graphviz.New(ctx)
	graph, _ := g.Graph()

	var dfs func(node *Operator)
	visited := make(map[*Operator]struct{})
	dfs = func(node *Operator) {
		if node == nil {
			return
		}
		fromName := fmt.Sprintf("%s[%d]", node.Name, node.maxDepth)
		from, _ := graph.CreateNodeByName(fromName)
		visited[node] = struct{}{}
		for _, nextNode := range node.NextList {
			toName := fmt.Sprintf("%s[%d]", nextNode.Name, nextNode.maxDepth)
			to, _ := graph.CreateNodeByName(toName)
			e, _ := graph.CreateEdgeByName("edge", from, to)
			_ = e
			if _, ok := visited[nextNode]; !ok {
				dfs(nextNode)
			}
		}
	}
	dfs(flow.start)

	f, _ := os.Create(svgFilePath)
	defer f.Close()
	return g.Render(ctx, graph, "svg", f)
}

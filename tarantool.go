package tarantool

import (
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/tarantool", new(Tarantool))
}

var (
	chCallFutures = make(chan *tarantool.Future, 4096)
)

// Tarantool is the k6 Tarantool extension
type Tarantool struct{}

func (Tarantool) ResolveCallFutures() {
	go func() {
		for fut := range chCallFutures {
			if _, err := fut.Get(); err != nil {
				panic(err)
			}
		}
	}()
}

// Connect creates a new Tarantool connection
func (Tarantool) Connect(addrs []string, opts tarantool.Opts) (*pool.ConnectionPool, error) {
	if len(addrs) == 0 {
		addrs = append(make([]string, 0), "localhost:3301")
	}
	conn, err := pool.Connect(addrs, opts)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Select performs select to box.space
func (Tarantool) Select(conn *pool.ConnectionPool, space, index interface{}, offset, limit uint32, iterator tarantool.Iter, key interface{}) (*tarantool.Response, error) {
	req := tarantool.NewSelectRequest(space).Index(index).Offset(offset).Limit(limit).Iterator(iterator).Key(key)

	return do(conn, req, pool.ANY)
}

// Insert performs insertion to box.space
func (Tarantool) Insert(conn *pool.ConnectionPool, space, data interface{}) (*tarantool.Response, error) {
	req := tarantool.NewInsertRequest(space).Tuple(data)

	return do(conn, req, pool.RW)
}

// Replace performs "insert or replace" action to box.space
func (Tarantool) Replace(conn *pool.ConnectionPool, space, data interface{}) (*tarantool.Response, error) {
	req := tarantool.NewReplaceRequest(space).Tuple(data)

	return do(conn, req, pool.RW)
}

// Delete performs deletion of a tuple by key
func (Tarantool) Delete(conn *pool.ConnectionPool, space, index, key interface{}) (*tarantool.Response, error) {
	req := tarantool.NewDeleteRequest(space).Index(index).Key(key)

	return do(conn, req, pool.RW)
}

// Update performs update of a tuple by key
func (Tarantool) Update(conn *pool.ConnectionPool, space, index, key, ops *tarantool.Operations) (*tarantool.Response, error) {
	req := tarantool.NewUpdateRequest(space).Index(index).Key(key).Operations(ops)

	return do(conn, req, pool.RW)
}

// Upsert performs "update or insert" action of a tuple by key
func (Tarantool) Upsert(conn *pool.ConnectionPool, space, tuple, ops *tarantool.Operations) (*tarantool.Response, error) {
	req := tarantool.NewUpsertRequest(space).Tuple(tuple).Operations(ops)

	return do(conn, req, pool.RW)
}

// Call calls registered tarantool function
func (Tarantool) Call(conn *pool.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	req := tarantool.NewCallRequest(fnName).Args(args)

	return do(conn, req, pool.ANY)
}

func (Tarantool) CallAsyncNoReturn(conn *pool.ConnectionPool, fnName string, args interface{}) {
	req := tarantool.NewCallRequest(fnName).Args(args)

	chCallFutures <- conn.Do(req, pool.ANY)
}

// Call17 calls registered tarantool function
func (Tarantool) Call17(conn *pool.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	req := tarantool.NewCall17Request(fnName).Args(args)

	return do(conn, req, pool.ANY)
}

// Eval passes lua expression for evaluation
func (Tarantool) Eval(conn *pool.ConnectionPool, expr string, args interface{}) (*tarantool.Response, error) {
	req := tarantool.NewEvalRequest(expr).Args(args)

	return do(conn, req, pool.ANY)
}

func do(conn *pool.ConnectionPool, req tarantool.Request, userMode pool.Mode) (*tarantool.Response, error) {
	resp, err := conn.Do(req, userMode).Get()
	if err != nil {
		return nil, err
	}
	return resp, err
}

package delay

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"

	"github.com/altipla-consulting/datetime"
	altiplaerrors "github.com/altipla-consulting/errors"
	"github.com/altipla-consulting/sentry"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	pb "github.com/altipla-consulting/delay/queues"
)

var (
	// registry of all delayed functions
	funcs = make(map[string]*Function)

	// precomputed types
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// Function is a stored task implementation.
type Function struct {
	fv  reflect.Value // Kind() == reflect.Func
	key string
	err error
}

// Func builds and registers a new task implementation.
func Func(key string, i interface{}) *Function {
	f := &Function{
		fv: reflect.ValueOf(i),
	}

	// Derive unique, somewhat stable key for this func.
	_, file, _, _ := runtime.Caller(1)
	f.key = file + ":" + key

	t := f.fv.Type()
	if t.Kind() != reflect.Func {
		f.err = fmt.Errorf("delay: not a function")
		return f
	}
	if t.NumIn() == 0 || t.In(0) != contextType {
		f.err = fmt.Errorf("delay: first argument must be context.Context")
		return f
	}

	// Register the function's arguments with the gob package.
	// This is required because they are marshaled inside a []interface{}.
	// gob.Register only expects to be called during initialization;
	// that's fine because this function expects the same.
	for i := 0; i < t.NumIn(); i++ {
		// Only concrete types may be registered. If the argument has
		// interface type, the client is resposible for registering the
		// concrete types it will hold.
		if t.In(i).Kind() == reflect.Interface {
			continue
		}
		gob.Register(reflect.Zero(t.In(i)).Interface())
	}

	if old := funcs[f.key]; old != nil {
		old.err = fmt.Errorf("delay: multiple functions registered for %s in %s", key, file)
	}
	funcs[f.key] = f

	return f
}

type invocation struct {
	Key  string
	Args []interface{}
}

// Task builds a task invocation to the function. You can later send the task
// in batches using queue.SendTasks() or directly invoke Call() to make both things
// at the same time.
func (f *Function) Task(args ...interface{}) (*pb.SendTask, error) {
	if f.err != nil {
		return nil, f.err
	}

	nArgs := len(args) + 1 // +1 for the context.Context
	ft := f.fv.Type()
	minArgs := ft.NumIn()
	if ft.IsVariadic() {
		minArgs--
	}
	if nArgs < minArgs {
		return nil, fmt.Errorf("delay: too few arguments to func: %d < %d", nArgs, minArgs)
	}
	if !ft.IsVariadic() && nArgs > minArgs {
		return nil, fmt.Errorf("delay: too many arguments to func: %d > %d", nArgs, minArgs)
	}

	// Check arg types.
	for i := 1; i < nArgs; i++ {
		at := reflect.TypeOf(args[i-1])

		var dt reflect.Type
		if i < minArgs {
			// not a variadic arg
			dt = ft.In(i)
		} else {
			// a variadic arg
			dt = ft.In(minArgs).Elem()
		}

		// nil arguments won't have a type, so they need special handling.
		if at == nil {
			// nil interface
			switch dt.Kind() {
			case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
				continue // may be nil
			}
			return nil, fmt.Errorf("delay: argument %d has wrong type: %v is not nilable", i, dt)
		}

		switch at.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			av := reflect.ValueOf(args[i-1])
			if av.IsNil() {
				// nil value in interface; not supported by gob, so we replace it
				// with a nil interface value
				args[i-1] = nil
			}
		}

		if !at.AssignableTo(dt) {
			return nil, fmt.Errorf("delay: argument %d has wrong type: %v is not assignable to %v", i, at, dt)
		}
	}

	inv := invocation{
		Key:  f.key,
		Args: args,
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(inv); err != nil {
		return nil, err
	}

	return &pb.SendTask{
		Payload: buf.Bytes(),
	}, nil
}

// Call builds a task invocation and directly sends it individually to the queue.
//
// If you are going to send multiple tasks at the same time is more efficient to
// build all of them with Task() first and then send them in batches with queue.SendTasks().
// If sending a single task this function will be similar in performance to the batch
// method described before.
func (f *Function) Call(ctx context.Context, queue QueueSpec, args ...interface{}) error {
	task, err := f.Task(args...)
	if err != nil {
		return err
	}

	return queue.SendTasks(ctx, []*pb.SendTask{task})
}

// Listener is a background goroutine that handles messages from the queues
// and run them in other controlled goroutines.
type Listener struct {
	sentryClient *sentry.Client
}

// NewListener prepares a new background goroutine to handle messages.
func NewListener(sentryDSN string) *Listener {
	lis := new(Listener)
	if sentryDSN != "" {
		lis.sentryClient = sentry.NewClient(sentryDSN)
	}

	return lis
}

// Handle opens a listen connection to the queue and starts receiving tasks from it
// in the background.
func (lis *Listener) Handle(queue QueueSpec) {
	go func() {
		for {
			if err := lis.listenQueue(queue); err != nil {
				log.WithFields(log.Fields{
					"error":   err.Error(),
					"project": queue.conn.project,
					"queue":   queue.name,
				}).Error("Error listening to queue, retrying in 15 seconds")
			}
			time.Sleep(15 * time.Second)
		}
	}()
}

func (lis *Listener) listenQueue(queue QueueSpec) error {
	group, ctx := errgroup.WithContext(context.Background())

	if queue.conn.redisClient != nil {
		group.Go(func() error {
			pubsub := queue.conn.redisClient.Subscribe(queue.name)

			var i int64
			for msg := range pubsub.Channel() {
				buf := proto.NewBuffer([]byte(msg.Payload))
				for {
					sendTask := new(pb.SendTask)
					if err := buf.DecodeMessage(sendTask); err != nil {
						if err == io.EOF {
							break
						}

						return fmt.Errorf("delay: cannot decode incoming task: %v", err)
					}

					i++
					task := &pb.Task{
						Code:    fmt.Sprintf("sim-%d", i),
						Payload: sendTask.Payload,
						Created: datetime.SerializeTimestamp(time.Now()),
						Retry:   0,
						Project: queue.conn.project,
						MinEta:  sendTask.MinEta,
					}

					log.WithFields(log.Fields{
						"project": task.Project,
						"queue":   task.QueueName,
						"task":    task.Code,
					}).Debug("Task received")

					if err := handleTask(ctx, task); err != nil {
						log.WithFields(log.Fields{
							"error":   err.Error(),
							"details": altiplaerrors.Details(err),
							"project": task.Project,
							"queue":   task.QueueName,
							"task":    task.Code,
						}).Error("Task handler failed")
					}
				}
			}

			return nil
		})
	} else {
		stream, err := queue.conn.queuesClient.Listen(ctx)
		if err != nil {
			return fmt.Errorf("delay: cannot listen to the queue: %v", err)
		}

		initial := &pb.ListenRequest{
			Request: &pb.ListenRequest_Initial{
				Initial: &pb.ListenInitial{
					Project:   queue.conn.project,
					QueueName: queue.name,
				},
			},
		}
		if err := stream.Send(initial); err != nil {
			return fmt.Errorf("delay: cannot send initial connection info: %v", err)
		}

		group.Go(func() error {
			for {
				reply, err := stream.Recv()
				if err != nil {
					return fmt.Errorf("delay: cannot receive tasks: %v", err)
				}

				group.Go(func() error {
					log.WithFields(log.Fields{
						"project": reply.Task.Project,
						"queue":   reply.Task.QueueName,
						"task":    reply.Task.Code,
					}).Debug("Task received")

					var failed bool
					if err := handleTask(ctx, reply.Task); err != nil {
						failed = true

						log.WithFields(log.Fields{
							"error":   err.Error(),
							"details": altiplaerrors.Details(err),
							"project": reply.Task.Project,
							"queue":   reply.Task.QueueName,
							"task":    reply.Task.Code,
						}).Error("Task handler failed")

						if lis.sentryClient != nil {
							lis.sentryClient.ReportInternal(ctx, err)
						}
					}

					req := &pb.ListenRequest{
						Request: &pb.ListenRequest_Ack{
							Ack: &pb.Ack{
								Code:    reply.Task.Code,
								Success: !failed,
							},
						},
					}
					if err := stream.Send(req); err != nil {
						return fmt.Errorf("delay: cannot ack task: %v", err)
					}

					return nil
				})
			}
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("delay: error closing the background queue goroutines: %v", err)
	}

	return nil
}

func handleTask(ctx context.Context, task *pb.Task) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	r := bytes.NewReader(task.Payload)
	var inv invocation
	if err := gob.NewDecoder(r).Decode(&inv); err != nil {
		return fmt.Errorf("delay: cannot decode call: %v", err)
	}

	f := funcs[inv.Key]
	if f == nil {
		return fmt.Errorf("delay: no func with key %q found", inv.Key)
	}

	ft := f.fv.Type()
	in := []reflect.Value{reflect.ValueOf(ctx)}
	for _, arg := range inv.Args {
		var v reflect.Value
		if arg != nil {
			v = reflect.ValueOf(arg)
		} else {
			// Task was passed a nil argument, so we must construct
			// the zero value for the argument here.
			n := len(in) // we're constructing the nth argument
			var at reflect.Type
			if !ft.IsVariadic() || n < ft.NumIn()-1 {
				at = ft.In(n)
			} else {
				at = ft.In(ft.NumIn() - 1).Elem()
			}
			v = reflect.Zero(at)
		}
		in = append(in, v)
	}
	out := f.fv.Call(in)

	if n := ft.NumOut(); n > 0 && ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			return fmt.Errorf("delay: handler failed: %v", errv.Interface().(error))
		}
	}

	return nil
}

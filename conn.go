package delay

import (
	"context"
	"crypto/tls"
	"fmt"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pbqueues "github.com/altipla-consulting/delay/queues"
)

const beauthTokenEndpoint = "https://beauth.io/token"

// Conn represents a connection to the queues server.
type Conn struct {
	project string
	client  pbqueues.QueuesServiceClient
}

// NewConn opens a new connection to a queues server. It needs the project and the OAuth
// client credentials to authenticate the requests.
func NewConn(project, clientID, clientSecret string) (*Conn, error) {
	config := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     beauthTokenEndpoint,
	}
	rpcCreds := grpc.WithPerRPCCredentials(oauthAccess{config.TokenSource(context.Background())})
	creds := credentials.NewTLS(&tls.Config{ServerName: "api-v3.altipla.consulting"})
	conn, err := grpc.Dial("api-v3.altipla.consulting:443", grpc.WithTransportCredentials(creds), rpcCreds)
	if err != nil {
		return nil, fmt.Errorf("delay: cannot connect to altipla api: %v", err)
	}

	return &Conn{
		project: project,
		client:  pbqueues.NewQueuesServiceClient(conn),
	}, nil
}

type oauthAccess struct {
	tokenSource oauth2.TokenSource
}

func (oa oauthAccess) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := oa.tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("delay: cannot update token: %v", err)
	}

	return map[string]string{
		"authorization": token.Type() + " " + token.AccessToken,
	}, nil
}

func (oa oauthAccess) RequireTransportSecurity() bool {
	return false
}

// QueueSpec contains a reference to a queue to send to and receive tasks from that queue.
type QueueSpec struct {
	conn *Conn
	name string
}

// Queue builds a new QueueSpec reference to a queue.
func Queue(conn *Conn, name string) QueueSpec {
	return QueueSpec{conn, name}
}

// SendTasks sends a list of tasks in batch to a queue.
func (queue QueueSpec) SendTasks(ctx context.Context, tasks []*pbqueues.SendTask) error {
	req := &pbqueues.SendTasksRequest{
		Project:   queue.conn.project,
		QueueName: queue.name,
		Tasks:     tasks,
	}
	var err error
	_, err = queue.conn.client.SendTasks(ctx, req)
	if err != nil {
		return fmt.Errorf("delay: cannot send tasks: %v", err)
	}

	return nil
}

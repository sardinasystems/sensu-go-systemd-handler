package service

import (
	"context"
	"fmt"
	"log"
	"net"

	systemdDBus "github.com/coreos/go-systemd/v22/dbus"
	"github.com/godbus/dbus/v5"
	sshtunnel "github.com/sgreben/sshtunnel/exec"
)

// DBusTunnel makes a tunnel socket->local-tcp
type DBusTunnel struct {
	ctx      context.Context
	ctxCf    context.CancelFunc
	listener net.Listener
	laddr    net.Addr
}

// NewDBusTunnel creates dbus socket tunnel
func NewDBusTunnel(ctx context.Context, tunnelConfig sshtunnel.Config, remoteSocket string) (*DBusTunnel, error) {
	// we're not going to support anything else
	tunnelConfig.CommandTemplate = sshtunnel.CommandTemplateOpenSSH

	laddr, err := getFreeLocalAddr()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local addr: %w", err)
	}

	ctx, cf := context.WithCancel(ctx)

	t := &DBusTunnel{
		ctx:   ctx,
		ctxCf: cf,
		laddr: laddr,
	}

	listener, errCh, err := sshtunnel.ListenContext(ctx, t.laddr, remoteSocket, &tunnelConfig)
	if err != nil {
		return nil, fmt.Errorf("tunnel failed to listen: %w", err)
	}
	go func() {
		err, ok := <-errCh
		if !ok {
			return
		}

		log.Fatalf("tunnel connection failed: %v", err)
	}()

	t.listener = listener
	return t, nil
}

// New makes d-bus connection to remote systemd
func (t *DBusTunnel) New() (*systemdDBus.Conn, error) {
	return systemdDBus.NewConnection(
		func() (*dbus.Conn, error) {
			return dbusAuthConnection(t.ctx, t.NewDBusConn)
		})
}

// NewDBusConn makes raw d-bus connection to the remote systemd
func (t *DBusTunnel) NewDBusConn(opts ...dbus.ConnOption) (*dbus.Conn, error) {
	host, port, err := net.SplitHostPort(t.laddr.String())
	if err != nil {
		return nil, fmt.Errorf("split laddr error: %w", err)
	}

	return dbus.Dial(fmt.Sprintf("tcp:host=%s,port=%s", host, port), opts...)
}

// copy from systemd/v22/dbus
func dbusAuthConnection(ctx context.Context, createBus func(opts ...dbus.ConnOption) (*dbus.Conn, error)) (*dbus.Conn, error) {
	conn, err := createBus(dbus.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	// Only use EXTERNAL method, and hardcode the uid (not username)
	// to avoid a username lookup (which requires a dynamically linked
	// libc)
	//methods := []dbus.Auth{dbus.AuthExternal(strconv.Itoa(os.Getuid()))}
	methods := []dbus.Auth{dbus.AuthExternal("0")}

	err = conn.Auth(methods)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func getFreeLocalAddr() (net.Addr, error) {
	laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return nil, err
	}
	defer l.Close()

	return l.Addr(), nil
}

// Close terminates ssh tunnel
func (t *DBusTunnel) Close() error {
	err := t.listener.Close()
	t.ctxCf()

	return err
}

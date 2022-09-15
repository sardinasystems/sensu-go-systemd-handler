package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	systemdDBus "github.com/coreos/go-systemd/v22/dbus"
	"github.com/fsnotify/fsnotify"
	"github.com/godbus/dbus/v5"
	"go.uber.org/multierr"
)

// DBusTunnelConfig stores config
type DBusTunnelConfig struct {
	User         string
	SSHHost      string
	SSHPort      int
	RemoteSocket string
	SSHVerbose   bool
}

// DBusTunnel makes a tunnel socket->local-tcp
type DBusTunnel struct {
	ctx    context.Context
	ctxCf  context.CancelFunc
	cfg    DBusTunnelConfig
	cmd    *exec.Cmd
	tmpdir string
	lsock  string
}

// NewDBusTunnel creates dbus socket tunnel
func NewDBusTunnel(ctx context.Context, tunnelConfig DBusTunnelConfig) (*DBusTunnel, error) {
	tempDir, err := os.MkdirTemp("", "ssh-tun*")
	if err != nil {
		return nil, err
	}

	lsock := filepath.Join(tempDir, "dbus.sock")

	ctx, cf := context.WithCancel(ctx)

	t := &DBusTunnel{
		ctx:    ctx,
		ctxCf:  cf,
		cfg:    tunnelConfig,
		tmpdir: tempDir,
		lsock:  lsock,
	}

	err = t.run()
	if err != nil {
		t.Close()
		return nil, err
	}

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
	return dbus.Dial(fmt.Sprintf("unix:path=%s", t.lsock), opts...)
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

// run starts ssh program
func (t *DBusTunnel) run() error {
	args := []string{
		//"ssh",
		"-nNT",
		"-L",
		fmt.Sprintf("%s:%s", t.lsock, t.cfg.RemoteSocket),
		"-p",
		fmt.Sprintf("%d", t.cfg.SSHPort),
		fmt.Sprintf("%s@%s", t.cfg.User, t.cfg.SSHHost),
	}

	for _, opts := range []string{
		"ForwardAgent=yes",
		"ControlMaster=auto",
		"ControlPersist=60s",
		"UserKnownHostsFile=/dev/null",
		"StrictHostKeyChecking=no",
		"ConnectTimeout=6",
		"ConnectionAttempts=30",
		"PreferredAuthentications=publickey",
	} {
		args = append(args, "-o", opts)
	}

	if t.cfg.SSHVerbose {
		args = append(args, "-v")
	}

	cmd := exec.CommandContext(t.ctx, "ssh", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if t.cfg.SSHVerbose {
		log.Printf("Starting: ssh %s", strings.Join(args, " "))
	}

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("command error: %w", err)
	}

	return t.waitForSocket()
}

func (t *DBusTunnel) waitForSocket() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("watcher new error: %w", err)
	}
	defer watcher.Close()

	err = watcher.Add(t.tmpdir)
	if err != nil {
		return fmt.Errorf("watcher add error: %w", err)
	}

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-watcher.Events:
			return nil

		case err := <-watcher.Errors:
			return fmt.Errorf("inotify error: %w", err)

		case <-timer.C:
			return fmt.Errorf("connection timeout")
		}
	}
}

// Close terminates ssh tunnel
func (t *DBusTunnel) Close() error {
	var err error

	if t.cmd != nil {
		err = multierr.Append(err, t.cmd.Process.Kill())
	}

	t.ctxCf()

	err = multierr.Append(err, os.RemoveAll(t.tmpdir))

	return err
}

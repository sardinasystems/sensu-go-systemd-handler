package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/sensu-community/sensu-plugin-sdk/sensu"
	"github.com/sensu/sensu-go/types"
	"go.uber.org/multierr"

	"github.com/sgreben/sshtunnel/backoff"
	sshtunnel "github.com/sgreben/sshtunnel/exec"

	"github.com/sardinasystems/sensu-go-systemd-handler/service"
)

// Config represents the handler plugin config.
type Config struct {
	sensu.PluginConfig
	UnitPatterns []string
	MatchUnits   bool
	Action       string
	Mode         string
	SSHHost      string
	SSHUser      string
	SSHPort      int
	BackoffMin   string
	BackoffMax   string
	Backoff      backoff.Config
	DBusSocket   string
}

var (
	plugin = Config{
		PluginConfig: sensu.PluginConfig{
			Name:     "sensu-go-systemd-handler",
			Short:    "A handler which can start/stop/restart unit(s) on entity's server",
			Keyspace: "sensu.io/plugins/sensu-go-systemd-handler/config",
		},
	}

	options = []*sensu.PluginConfigOption{
		{
			Path:      "unit",
			Env:       "SYSTEMD_UNIT",
			Argument:  "unit",
			Shorthand: "s",
			Usage:     "Systemd unit(s) names/patterns to action",
			Value:     &plugin.UnitPatterns,
		},
		{
			Path:      "match",
			Env:       "SYSTEMD_MATCH_UNITS",
			Argument:  "match",
			Shorthand: "m",
			Usage:     "Match unit(s) patterns",
			Value:     &plugin.MatchUnits,
		},
		{
			Path:      "action",
			Env:       "SYSTEMD_ACTION",
			Argument:  "action",
			Shorthand: "a",
			Usage:     "Action to perform: start, stop, restart, reload",
			Value:     &plugin.Action,
			Default:   "restart",
		},
		{
			Path:      "mode",
			Env:       "SYSTEMD_MODE",
			Argument:  "mode",
			Shorthand: "M",
			Usage:     "Action mode: replace, fail, isolate, ignore-dependencies, ignore-requirements",
			Value:     &plugin.Mode,
			Default:   "replace",
		},
		{
			Path:      "ssh_host",
			Argument:  "ssh-host",
			Shorthand: "H",
			Usage:     "SSH host (default: entity.hostname)",
			Value:     &plugin.SSHHost,
			Default:   "",
		},
		{
			Path:      "ssh_user",
			Argument:  "ssh-user",
			Shorthand: "u",
			Usage:     "SSH User",
			Value:     &plugin.SSHUser,
			Default:   "root",
		},
		{
			Path:      "ssh_port",
			Argument:  "ssh-port",
			Shorthand: "p",
			Usage:     "SSH Port",
			Value:     &plugin.SSHPort,
			Default:   22,
		},
		{
			Path:     "ssh_min_delay",
			Argument: "ssh-min-delay",
			Usage:    "Minimum re-connection attempt delay",
			Value:    &plugin.BackoffMin,
			Default:  "250ms",
		},
		{
			Path:     "ssh_max_delay",
			Argument: "ssh-max-delay",
			Usage:    "Maximum re-connection attempt delay",
			Value:    &plugin.BackoffMax,
			Default:  "10s",
		},
		{
			Path:     "ssh_max_attempts",
			Argument: "ssh-max-attempts",
			Usage:    "Maximum number of re-connection attempts",
			Value:    &plugin.Backoff.MaxAttempts,
			Default:  3,
		},
		{
			Path:     "dbus_socket",
			Argument: "dbus-socket",
			Usage:    "Remote D-BUS socket path",
			Value:    &plugin.DBusSocket,
			Default:  "/var/run/systemd/private",
		},
	}
)

func main() {
	handler := sensu.NewGoHandler(&plugin.PluginConfig, options, checkArgs, executeHandler)
	handler.Execute()
}

func stringsContains(sl []string, s string) bool {
	for _, ss := range sl {
		if s == ss {
			return true
		}
	}

	return false
}

type actionFunc func(name string, mode string, ch chan<- string) (int, error)

func getActionFunc(conn *dbus.Conn) (actionFunc, error) {
	switch plugin.Action {
	case "start":
		return conn.StartUnit, nil

	case "stop":
		return conn.StopUnit, nil

	case "restart":
		return conn.RestartUnit, nil

	case "reload":
		return conn.ReloadUnit, nil

	case "try-restart":
		return conn.TryRestartUnit, nil

	case "reload-or-restart":
		return conn.ReloadOrRestartUnit, nil

	case "reload-or-try-restart":
		return conn.ReloadOrTryRestartUnit, nil

	default:
		return nil, fmt.Errorf("unsupported action: %s", plugin.Action)
	}
}

func checkArgs(_ *types.Event) error {
	allowedActions := []string{"start", "stop", "restart", "reload", "try-restart", "reload-or-restart", "reload-or-try-restart"}
	allowedModes := []string{"replace", "fail", "isolate", "ignore-dependencies", "ignore-requirements"}
	var err error

	if len(plugin.UnitPatterns) == 0 {
		return fmt.Errorf("--unit or SYSTEMD_UNIT environment variable is required")
	}
	if !stringsContains(allowedActions, plugin.Action) {
		return fmt.Errorf("--action must be one of %v, but it is: %v", allowedActions, plugin.Action)
	}
	if !stringsContains(allowedModes, plugin.Mode) {
		return fmt.Errorf("--mode must be one of %v, but it is: %v", allowedModes, plugin.Mode)
	}

	plugin.Backoff.Min, err = time.ParseDuration(plugin.BackoffMin)
	if err != nil {
		return fmt.Errorf("Duration parse error: %w", err)
	}
	plugin.Backoff.Max, err = time.ParseDuration(plugin.BackoffMax)
	if err != nil {
		return fmt.Errorf("Duration parse error: %w", err)
	}

	return nil
}

func executeHandler(event *types.Event) error {
	tunnelConfig := sshtunnel.Config{
		User:    plugin.SSHUser,
		SSHHost: plugin.SSHHost,
		SSHPort: strconv.Itoa(plugin.SSHPort),
		Backoff: plugin.Backoff,
	}

	if tunnelConfig.SSHHost == "" {
		tunnelConfig.SSHHost = event.Entity.System.Hostname
	}

	ctx := context.Background()

	log.Printf("Connecting ssh tunnel to: %s:%s", tunnelConfig.SSHHost, tunnelConfig.SSHPort)
	stun, err := service.NewDBusTunnel(ctx, tunnelConfig, plugin.DBusSocket)
	if err != nil {
		return fmt.Errorf("SSH Tunnel error: %w", err)
	}
	defer stun.Close()

	conn, err := stun.New()
	if err != nil {
		return fmt.Errorf("D-BUS error: %w", err)
	}

	unitNames := make([]string, 0)

	if plugin.MatchUnits {
		log.Printf("Matching unit patterns...")

		rawConn, err := stun.NewDBusConn()
		if err != nil {
			return fmt.Errorf("failed to make raw d-bus connection: %w", err)
		}

		unitFetcher, err := service.InstrospectForUnitMethods(rawConn)
		if err != nil {
			return fmt.Errorf("could not introspect systemd dbus: %w", err)
		}

		unitStats, err := unitFetcher(conn, nil, plugin.UnitPatterns)
		if err != nil {
			return fmt.Errorf("list units error: %w", err)
		}

		for _, unit := range unitStats {
			unitNames = append(unitNames, unit.Name)
		}
	} else {
		log.Printf("Use unit names as-is")
		for _, unit := range plugin.UnitPatterns {
			unitNames = append(unitNames, unit)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, len(unitNames))
	for idx, unitName := range unitNames {
		log.Printf("%s: Triggering %s action (%d/%d)", unitName, plugin.Action, idx, len(unitNames))
		wg.Add(1)
		go func(unitName string) {
			defer wg.Done()

			af, err2 := getActionFunc(conn)
			if err2 != nil {
				errors <- err2
			}

			resultCh := make(chan string)

			_, err2 = af(unitName, plugin.Mode, resultCh)
			if err2 != nil {
				log.Printf("%s: Action error: %v", unitName, err2)
				errors <- err2
				return
			}

			result := <-resultCh
			close(resultCh)

			log.Printf("%s: result: %s", unitName, result)
		}(unitName)
	}

	wg.Wait()
	close(errors)

	for err2 := range errors {
		err = multierr.Append(err, err2)
	}

	return err
}

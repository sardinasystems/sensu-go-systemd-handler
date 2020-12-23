package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/sensu-community/sensu-plugin-sdk/sensu"
	"github.com/sensu/sensu-go/types"
	"go.uber.org/multierr"

	"github.com/sardinasystems/sensu-go-systemd-handler/service"
)

// Config represents the handler plugin config.
type Config struct {
	sensu.PluginConfig
	UnitPatterns []string
	MatchUnits   bool
	Action       string
	Mode         string
	Tun          service.DBusTunnelConfig
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
			Value:     &plugin.Tun.SSHHost,
			Default:   "",
		},
		{
			Path:      "ssh_user",
			Argument:  "ssh-user",
			Shorthand: "u",
			Usage:     "SSH User",
			Value:     &plugin.Tun.User,
			Default:   "root",
		},
		{
			Path:      "ssh_port",
			Argument:  "ssh-port",
			Shorthand: "p",
			Usage:     "SSH Port",
			Value:     &plugin.Tun.SSHPort,
			Default:   22,
		},
		{
			Path:     "ssh_verbose",
			Argument: "ssh-verbose",
			Usage:    "SSH Verbose mode (for debugging)",
			Value:    &plugin.Tun.SSHVerbose,
		},
		{
			Path:     "dbus_socket",
			Argument: "dbus-socket",
			Usage:    "Remote D-BUS socket path",
			Value:    &plugin.Tun.RemoteSocket,
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

	if len(plugin.UnitPatterns) == 0 {
		return fmt.Errorf("--unit or SYSTEMD_UNIT environment variable is required")
	}
	if !stringsContains(allowedActions, plugin.Action) {
		return fmt.Errorf("--action must be one of %v, but it is: %v", allowedActions, plugin.Action)
	}
	if !stringsContains(allowedModes, plugin.Mode) {
		return fmt.Errorf("--mode must be one of %v, but it is: %v", allowedModes, plugin.Mode)
	}

	return nil
}

func executeHandler(event *types.Event) error {
	ctx := context.Background()

	if plugin.Tun.SSHHost == "" {
		plugin.Tun.SSHHost = event.Entity.System.Hostname
	}

	log.Printf("Connecting ssh tunnel to: %s:%d", plugin.Tun.SSHHost, plugin.Tun.SSHPort)
	stun, err := service.NewDBusTunnel(ctx, plugin.Tun)
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

		// NOTE(vermakov): use local systemd to introspect remote methods
		unitFetcher, err := service.InstrospectForUnitMethods(nil)
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
		log.Printf("%s: Triggering %s action (%d/%d)", unitName, plugin.Action, idx+1, len(unitNames))
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

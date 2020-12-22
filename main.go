package main

import (
	"fmt"
	"log"
	"time"

	"github.com/sensu-community/sensu-plugin-sdk/sensu"
	"github.com/sensu/sensu-go/types"

	"github.com/sgreben/sshtunnel/backoff"
	//"github.com/sgreben/sshtunnel/exec"
)

// Config represents the handler plugin config.
type Config struct {
	sensu.PluginConfig
	UnitPatterns []string
	Action       string
	SSHUser      string
	SSHPort      int
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
			Usage:     "Systemd unit(s) pattern to action",
			Value:     &plugin.UnitPatterns,
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
			Value:    &plugin.Backoff.Min,
			Default:  250 * time.Milliseconds,
		},
		{
			Path:     "ssh_max_delay",
			Argument: "ssh-max-delay",
			Usage:    "Maximum re-connection attempt delay",
			Value:    &plugin.Backoff.Max,
			Default:  10 * time.Seconds,
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

func checkArgs(_ *types.Event) error {
	allowedActions := []string{"start", "stop", "restart", "reload"}

	if len(plugin.UnitPatterns) == 0 {
		return fmt.Errorf("--unit or SYSTEMD_UNIT environment variable is required")
	}
	if !stringsContains(allowedActions, plugin.Action) {
		return fmt.Errorf("--action must be one of %v, but it is: %v", allowedActions, plugin.Action)
	}

	return nil
}

func executeHandler(event *types.Event) error {
	log.Println("executing handler with --unit", plugin.UnitPatterns)

	return nil
}

// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build !netbsd
// +build !netbsd

package service

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusRaw "github.com/godbus/dbus/v5"
)

// UnitFetcher a unit retrieval method
type UnitFetcher func(ctx context.Context, conn *dbus.Conn, states, patterns []string) ([]dbus.UnitStatus, error)

// InstrospectForUnitMethods determines what methods are available via dbus for listing systemd units.
// We have a number of functions, some better than others, for getting and filtering unit lists.
// This will attempt to find the most optimal method, and move down to methods that require more work.
func InstrospectForUnitMethods(conn *dbusRaw.Conn) (UnitFetcher, error) {
	var err error

	if conn == nil {
		//setup a dbus connection
		conn, err = dbusRaw.SystemBusPrivate()
		if err != nil {
			return nil, fmt.Errorf("error getting connection to system bus: %w", err)
		}
	}

	auth := dbusRaw.AuthExternal(strconv.Itoa(os.Getuid()))
	//auth := dbusRaw.AuthExternal("0")
	err = conn.Auth([]dbusRaw.Auth{auth})
	if err != nil {
		return nil, fmt.Errorf("authentication error: %w", err)
	}

	err = conn.Hello()
	if err != nil {
		return nil, fmt.Errorf("error in Hello: %w", err)
	}

	var props string

	//call "introspect" on the systemd1 path to see what ListUnit* methods are available
	obj := conn.Object("org.freedesktop.systemd1", dbusRaw.ObjectPath("/org/freedesktop/systemd1"))
	err = obj.Call("org.freedesktop.DBus.Introspectable.Introspect", 0).Store(&props)
	if err != nil {
		return nil, fmt.Errorf("dbus call error: %w", err)
	}

	unitMap, err := parseXMLAndReturnMethods(props)
	if err != nil {
		return nil, fmt.Errorf("error handling XML: %w", err)
	}

	//return a function callback ordered by desirability
	if _, ok := unitMap["ListUnitsByPatterns"]; ok {
		return listUnitsByPatternWrapper, nil
	} else if _, ok := unitMap["ListUnitsFiltered"]; ok {
		return listUnitsFilteredWrapper, nil
	} else if _, ok := unitMap["ListUnits"]; ok {
		return listUnitsWrapper, nil
	}
	return nil, fmt.Errorf("no supported list Units function: %v", unitMap)
}

func parseXMLAndReturnMethods(str string) (map[string]bool, error) {

	type Method struct {
		Name string `xml:"name,attr"`
	}

	type Iface struct {
		Name   string   `xml:"name,attr"`
		Method []Method `xml:"method"`
	}

	type IntrospectData struct {
		XMLName   xml.Name `xml:"node"`
		Interface []Iface  `xml:"interface"`
	}

	methods := IntrospectData{}

	err := xml.Unmarshal([]byte(str), &methods)
	if err != nil {
		return nil, fmt.Errorf("unmarshal XML error: %w", err)
	}

	if len(methods.Interface) == 0 {
		return nil, fmt.Errorf("no methods found on introspect: %w", err)
	}
	methodMap := make(map[string]bool)
	for _, iface := range methods.Interface {
		for _, method := range iface.Method {
			if strings.Contains(method.Name, "ListUnits") {
				methodMap[method.Name] = true
			}
		}
	}

	return methodMap, nil
}

// listUnitsByPatternWrapper is a bare wrapper for the unitFetcher type
func listUnitsByPatternWrapper(ctx context.Context, conn *dbus.Conn, states, patterns []string) ([]dbus.UnitStatus, error) {
	return conn.ListUnitsByPatternsContext(ctx, states, patterns)
}

//listUnitsFilteredWrapper wraps the dbus ListUnitsFiltered method
func listUnitsFilteredWrapper(ctx context.Context, conn *dbus.Conn, states, patterns []string) ([]dbus.UnitStatus, error) {
	units, err := conn.ListUnitsFilteredContext(ctx, states)
	if err != nil {
		return nil, fmt.Errorf("ListUnitsFiltered error: %w", err)
	}

	return MatchUnitPatterns(patterns, units)
}

// listUnitsWrapper wraps the dbus ListUnits method
func listUnitsWrapper(ctx context.Context, conn *dbus.Conn, states, patterns []string) ([]dbus.UnitStatus, error) {
	units, err := conn.ListUnitsContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("ListUnits error: %w", err)
	}
	if len(patterns) > 0 {
		units, err = MatchUnitPatterns(patterns, units)
		if err != nil {
			return nil, fmt.Errorf("matching unit patterns error: %w", err)
		}
	}

	if len(states) > 0 {
		var finalUnits []dbus.UnitStatus
		for _, unit := range units {
			for _, state := range states {
				if unit.LoadState == state || unit.ActiveState == state || unit.SubState == state {
					finalUnits = append(finalUnits, unit)
					break
				}
			}
		}
		return finalUnits, nil
	}

	return units, nil
}

// MatchUnitPatterns returns a list of units that match the pattern list.
// This algo, including filepath.Match, is designed to (somewhat) emulate the behavior of ListUnitsByPatterns, which uses `fnmatch`.
func MatchUnitPatterns(patterns []string, units []dbus.UnitStatus) ([]dbus.UnitStatus, error) {
	var matchUnits []dbus.UnitStatus
	for _, unit := range units {
		for _, pattern := range patterns {
			match, err := filepath.Match(pattern, unit.Name)
			if err != nil {
				return nil, fmt.Errorf("matching with pattern %s error: %w", pattern, err)
			}
			if match {
				matchUnits = append(matchUnits, unit)
				break
			}
		}
	}
	return matchUnits, nil
}

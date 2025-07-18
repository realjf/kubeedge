/*
Copyright 2018 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/kubernetes/pkg/apis/core/validation"

	"github.com/kubeedge/kubeedge/common/constants"
)

func GetLocalIP(hostName string) (string, error) {
	var ipAddr net.IP
	var err error

	// fix https://github.com/kubeedge/kubeedge/issues/6023
	// LookupIP maybe get docker0 ip's or other cni ip's
	// maybe get default route interface ip's can be safter
	// if get default ip fails then should use LookupIP
	// ignore when ChooseHostInterface fails
	ipAddr, err = utilnet.ChooseHostInterface()
	if err == nil {
		return ipAddr.String(), nil
	}

	// if ChooseHostInterface fails we can use LookupIP
	addrs, _ := net.LookupIP(hostName)
	for _, addr := range addrs {
		if err := ValidateNodeIP(addr); err != nil {
			continue
		}
		if addr.To4() != nil {
			ipAddr = addr
			break
		}
		if ipAddr == nil && addr.To16() != nil {
			ipAddr = addr
		}
	}
	if ipAddr == nil {
		return "", fmt.Errorf("can not get any useable node IP")
	}

	return ipAddr.String(), nil
}

// ValidateNodeIP validates given node IP belongs to the current host
func ValidateNodeIP(nodeIP net.IP) error {
	// Honor IP limitations set in setNodeStatus()
	if nodeIP.To4() == nil && nodeIP.To16() == nil {
		return fmt.Errorf("nodeIP must be a valid IP address")
	}
	if nodeIP.IsLoopback() {
		return fmt.Errorf("nodeIP can't be loopback address")
	}
	if nodeIP.IsMulticast() {
		return fmt.Errorf("nodeIP can't be a multicast address")
	}
	if nodeIP.IsLinkLocalUnicast() {
		return fmt.Errorf("nodeIP can't be a link-local unicast address")
	}
	if nodeIP.IsUnspecified() {
		return fmt.Errorf("nodeIP can't be an all zeros address")
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip != nil && ip.Equal(nodeIP) {
			return nil
		}
	}
	return fmt.Errorf("node IP: %q not found in the host's network interfaces", nodeIP.String())
}

// Command executes command and returns output
func Command(name string, arg []string) (string, error) {
	cmd := exec.Command(name, arg...)
	ret, err := cmd.Output()
	if err != nil {
		return string(ret), err
	}
	return strings.Trim(string(ret), "\n"), nil
}

// GetCurPath returns filepath
func GetCurPath() string {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	rst := filepath.Dir(path)
	return rst
}

func SpliceErrors(errors []error) string {
	if len(errors) == 0 {
		return ""
	}
	var stb strings.Builder
	stb.WriteString("[\n")
	for _, err := range errors {
		stb.WriteString(fmt.Sprintf("  %s\n", err.Error()))
	}
	stb.WriteString("]\n")
	return stb.String()
}

// GetHostname returns a reasonable hostname
func GetHostname() string {
	hostnameOverride, err := nodeutil.GetHostname("")
	if err != nil {
		return constants.DefaultHostnameOverride
	}
	msgs := validation.ValidateNodeName(hostnameOverride, false)
	if len(msgs) > 0 {
		return constants.DefaultHostnameOverride
	}
	return hostnameOverride
}

// ConcatStrings use bytes.buffer to concatenate string variable
func ConcatStrings(ss ...string) string {
	var bff bytes.Buffer
	for _, s := range ss {
		bff.WriteString(s)
	}
	return bff.String()
}

// GetResourceID return resource ID
func GetResourceID(namespace, name string) string {
	return namespace + "/" + name
}

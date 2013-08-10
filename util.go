// Copyright 2013 M-Lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package rtt provides a resolver for mlab-ns2 using RTT-based metrics.
package rtt

import (
	"net"
)

const (
	v4PrefixSize = 22 // defines the size of the IPv4 group prefix
	v6PrefixSize = 56 // defines the size of the IPv6 group prefix
)

var (
	v4PrefixMask = net.CIDRMask(v4PrefixSize, 8*net.IPv4len)
	v6PrefixMask = net.CIDRMask(v6PrefixSize, 8*net.IPv6len)
)

// GetClientGroup returns a *net.IPNet which represents a subnet of prefix
// size v4PrefixSize in the case of IPv4 addresses.
func GetClientGroup(ip net.IP) *net.IPNet {
	if ip.To4() == nil {
		return &net.IPNet{IP: ip.Mask(v6PrefixMask), Mask: v6PrefixMask}
	}
	return &net.IPNet{IP: ip.Mask(v4PrefixMask), Mask: v4PrefixMask}
}

// IsEqualClientGroup checks if two IPs are in the same client group defined
// by prefix sizes defined by v4PrefixSize and v6PrefixSize.
func IsEqualClientGroup(a, b net.IP) bool {
	ipnet := GetClientGroup(a)
	return ipnet.Contains(b)
}

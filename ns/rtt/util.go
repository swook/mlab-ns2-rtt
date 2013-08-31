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
	"errors"
	"net"
	"sort"
	"time"
)

const (
	v4PrefixSize = 22 // defines the size of the IPv4 group prefix
	v6PrefixSize = 56 // defines the size of the IPv6 group prefix
)

var (
	v4PrefixMask        = net.CIDRMask(v4PrefixSize, 8*net.IPv4len)
	v6PrefixMask        = net.CIDRMask(v6PrefixSize, 8*net.IPv6len)
	ErrMergeSiteRTT     = errors.New("SiteRTT cannot be merged, mismatching Site IDs.")
	ErrMergeClientGroup = errors.New("ClientGroups cannot be merged, mismatching ClientGroup Prefixes.")
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

// MergeSiteRTTs merges a new SiteRTT entry into an old SiteRTT entry if the new
// entry has lower or equal RTT, and also reports whether the merge has caused
// any changes.
func MergeSiteRTTs(oldSR, newSR *SiteRTT) (bool, error) {
	if oldSR.SiteID != newSR.SiteID {
		return false, ErrMergeSiteRTT
	}
	if newSR.RTT <= oldSR.RTT {
		oldSR.RTT = newSR.RTT
		oldSR.LastUpdated = newSR.LastUpdated
		return true, nil
	}
	return false, nil
}

// MergeClientGroups merges a new list of SiteRTT with an existing list of
// SiteRTT and sorts it in ascending RTT order. It also reports if the merge has
// caused any changes.
// Note: Used for merging new bigquery data with existing datastore data.
func MergeClientGroups(oldCG, newCG *ClientGroup) (bool, error) {
	oIP, nIP := net.IP(oldCG.Prefix), net.IP(newCG.Prefix)
	if !oIP.Equal(nIP) {
		return false, ErrMergeClientGroup
	}

	// Populate temporary maps to ease merge
	oRTTs := make(map[string]*SiteRTT)
	nRTTs := make(map[string]*SiteRTT)
	for i, s := range oldCG.SiteRTTs {
		oRTTs[s.SiteID] = &oldCG.SiteRTTs[i]
	}
	for i, s := range newCG.SiteRTTs {
		nRTTs[s.SiteID] = &newCG.SiteRTTs[i]
	}

	// Keep SiteRTT with lower RTT
	var os *SiteRTT
	var ok, changed, srChanged bool
	var err error
	for k, ns := range nRTTs {
		os, ok = oRTTs[k]
		if !ok {
			oRTTs[k] = ns
			changed = true
		} else {
			srChanged, err = MergeSiteRTTs(os, ns)
			if err != nil {
				return false, err
			}
			if srChanged {
				changed = true
			}
		}
	}

	// Create new list of SiteRTTs
	oldCG.SiteRTTs = make(SiteRTTs, 0, len(oRTTs))
	for _, s := range oRTTs {
		oldCG.SiteRTTs = append(oldCG.SiteRTTs, *s)
	}
	sort.Sort(oldCG.SiteRTTs)

	return changed, nil
}

// RetryWithExpDelay repeats the calling of a function f with an exponentially
// increasing delay starting from 2 seconds and doubling per iteration.
//
// The input function f can be provided by using a closure such as:
//	func() error {
//		ActualFunction(localVariables)
//	}
//
// A logging function can be provided in the following way for App Engine:
//	retryWithExpDelay(f, c.Infof, "BQImport", 0)
//
// while for other usage, a closure can be passed:
//	retryWithExpDelay(f, func(str string, v ...interface{}) {
//		fmt.Printf(str, v...)
//	}, "BQImport", 0)
// Note: func signature for fmt.Printf is func(string, ...interface{}) (int, error)
//
func RetryWithExpDelay(f func() error, logf func(string, ...interface{}), jobName string, delay int) {
	if delay > 0 {
		<-time.After(time.Duration(int64(delay) * int64(time.Second)))
	} else {
		delay = 1
	}
	err := f()
	if err != nil {
		delay *= 2
		logf("%s: Retrying after %d seconds.\n", jobName, delay)
		RetryWithExpDelay(f, logf, jobName, delay)
	}
	return
}

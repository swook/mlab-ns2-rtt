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

package rtt

const (
	URLMain = "/rtt/"

	URLTaskImportDay         = "/admin/tasks/rtt/import/day"
	URLTaskImportPut         = "/admin/tasks/rtt/put"
	URLSetLastSuccImportDate = "/admin/rtt/import/setLastSuccessfulDate"

	URLImportDay   = "/admin/rtt/import/day"
	URLImportDaily = "/admin/rtt/import/daily"
	URLImportAll   = "/admin/rtt/import/all"

	TaskQueueNameImport    = "rtt-import"
	TaskQueueNameImportPut = "rtt-import-put"

	FormKeyImportDate = "date"
	FormKeyPutKey     = "key"

	DSKeyLastSuccImport = "rtt:LastSuccessfulImport"
)

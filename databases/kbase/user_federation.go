// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package kbase

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/kbase/dts/config"
)

//=======================
// KBase user federation
//=======================

// In order to map an ORCID to a KBase username, we maintain a mapping that
// stores entries for all KBase users with ORCIDs. This mapping currently lives
// a 2-column spreadsheet (kbase_user_orcids.csv) in the DTS data directory.
// The data in this spreadsheet is reloaded every hour on the top of the hour
// so a new file can be dropped into the data directory with predictable results.

// starts up the user federation machinery if it hasn't yet been started
func startUserFederation() error {
	// fire up the user federation goroutine if needed
	if !kbaseUserFederationStarted {
		started := make(chan struct{})
		go kbaseUserFederation(started)
		<-started // wait for it to start

		// load the user table
		kbaseUpdateChan <- struct{}{}
		err := <-kbaseErrorChan
		if err != nil {
			return err
		}

		// start a pulse that reloads the user table from a file at the top of every hour
		go func() {
			for {
				t := time.Now()
				topOfHour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
				time.Sleep(time.Until(topOfHour))
				kbaseUpdateChan <- struct{}{}

				// reloading errors are logged, not propagated
				err := <-kbaseErrorChan
				if err != nil {
					slog.Warn(err.Error())
				}
			}
		}()
	}
	return nil
}

// returns the KBase username associated with the given ORCID
func usernameForOrcid(orcid string) (string, error) {
	if !kbaseUserFederationStarted {
		return "", fmt.Errorf("KBase federated user table not available!")
	}
	kbaseOrcidChan <- orcid
	username := <-kbaseUserChan
	err := <-kbaseErrorChan
	return username, err
}

// stops the user federation machinery
func stopUserFederation() error {
	if !kbaseUserFederationStarted {
		return fmt.Errorf("KBase user federation not started!")
	}
	kbaseStopChan <- struct{}{}
	err := <-kbaseErrorChan
	return err
}

//-----------
// Internals
//-----------

var kbaseUserFederationStarted = false
var kbaseUpdateChan chan struct{} // triggers updates to the ORCID/user table
var kbaseStopChan chan struct{}   // stops the user federation subsystem
var kbaseOrcidChan chan string    // passes ORCIDs in for lookup
var kbaseUserChan chan string     // passes usernames out
var kbaseErrorChan chan error     // passes errors out

// This goroutine maintains a table that associates ORCIDs with KBase users.
// It fields requests for usernames given ORCIDs, and can also update the table
// by reading a file.
func kbaseUserFederation(started chan struct{}) {
	// channels
	kbaseOrcidChan = make(chan string)
	kbaseUserChan = make(chan string)
	kbaseErrorChan = make(chan error)
	kbaseUpdateChan = make(chan struct{})
	kbaseStopChan = make(chan struct{})

	// mapping of ORCIDs to KBase users
	kbaseUserTable := make(map[string]string)

	// we're ready
	kbaseUserFederationStarted = true
	started <- struct{}{}

	for {
		select {
		case orcid := <-kbaseOrcidChan: // fetching username for orcid
			if username, found := kbaseUserTable[orcid]; found {
				kbaseUserChan <- username
				kbaseErrorChan <- nil
			} else {
				kbaseUserChan <- ""
				kbaseErrorChan <- fmt.Errorf("KBase user not found for ORCID %s!", orcid)
			}
		case <-kbaseUpdateChan: // update ORCID/user table
			var err error
			newUserTable, err := readUserTable("kbase_user_orcids.csv")
			if err == nil {
				kbaseUserTable = newUserTable
			}
			kbaseErrorChan <- err
		case <-kbaseStopChan: // stop the subsystem
			kbaseUserFederationStarted = false
			kbaseErrorChan <- nil
			break
		}
	}
}

// reads the specified .csv file within the DTS data directory, returning a map
// with ORCID keys associated with username values
func readUserTable(csvFile string) (map[string]string, error) {
	// open the CVS file containing the user mapping
	filename := filepath.Join(config.Service.DataDirectory, csvFile)
	file, err := os.Open(filename)
	if err != nil {
		return nil, InvalidKBaseUserSpreadsheet{
			File:    csvFile,
			Message: "nonexistent file",
		}
	}
	defer file.Close()

	// Scan the file line by line. Each line should contain 2 cells separated
	// by a comma. The first line is almost certainly a header with column names,
	// but we can't be sure, so we simply read every line, checking that
	//
	// * there are 2 entries separated by exactly one comma
	// * exactly one of the entries is a well-formed ORCID (xxxx-xxxx-xxxx-xxxx)
	// * the other entry is a non-empty string with no special characters
	//
	// Finally, the structure of all the lines in the file must agree. Every line
	// that doesn't conform to these requirements is ignored. If there's at least
	// one valid line, we clear the existing KBase user table and add each
	// (ORCID, user) pair to the user table.
	orcidColumn := -1
	userColumn := -1
	orcidsForUsers := make(map[string]string)
	usersForOrcids := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		cells := strings.Split(line, ",")
		if len(cells) != 2 {
			return nil, InvalidKBaseUserSpreadsheet{
				File:    csvFile,
				Message: fmt.Sprintf("%d comma-separated columns found (2 expected)", len(cells)),
			}
		}

		if orcidColumn == -1 { // find the column with an ORCID
			for i := 0; i < 2; i++ {
				if isOrcid(cells[i]) {
					orcidColumn = i
					userColumn = (i + 1) % 2 // user column's the other one
				}
			}
		} else if !isOrcid(cells[orcidColumn]) {
			// we've already established the ORCID column, but this line disagrees,
			// so the whole file is suspect
			return nil, InvalidKBaseUserSpreadsheet{
				File:    csvFile,
				Message: "Different lines list username, ORCID data in different columns",
			}
		}

		if orcidColumn != -1 {
			orcid := cells[orcidColumn]
			// ORCID column's okay, but what about the user column?
			if !isUsername(cells[userColumn]) {
				continue
			}
			username := cells[userColumn]

			// have we seen this ORCID or username before? It's okay, as long as everything
			// is consistent
			if existingUser, found := usersForOrcids[orcid]; found {
				if existingUser != orcid {
					return nil, InvalidKBaseUserSpreadsheet{
						File:    csvFile,
						Message: fmt.Sprintf("ORCID %s is associated with multiple users", orcid),
					}
				}
			} else {
				usersForOrcids[orcid] = username
			}
			if existingOrcid, found := orcidsForUsers[username]; found {
				if existingOrcid != orcid {
					return nil, InvalidKBaseUserSpreadsheet{
						File:    csvFile,
						Message: fmt.Sprintf("User %s has multiple ORCIDs", username),
					}
				}
			} else {
				orcidsForUsers[username] = orcid
			}
		}
	}

	if len(usersForOrcids) == 0 {
		return nil, InvalidKBaseUserSpreadsheet{
			File:    csvFile,
			Message: "No valid username/ORCID pairs found",
		}
	}

	return usersForOrcids, nil
}

// returns true iff s contains a valid username
func isUsername(s string) bool {
	return len(s) > 0 && !strings.ContainsFunc(s, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_'
	})
}

// returns true iff s contains a valid ORCID
func isOrcid(s string) bool {
	if len(s) != 19 {
		return false
	}
	if s[4] != '-' || s[9] != '-' || s[14] != '-' {
		return false
	}
	isAllDigits := func(s string) bool {
		return !strings.ContainsFunc(s, func(c rune) bool { return !unicode.IsDigit(c) })
	}
	if !isAllDigits(s[:4]) || !isAllDigits(s[5:9]) || !isAllDigits(s[10:14]) || !isAllDigits(s[15:18]) {
		return false
	}
	// the last character can be either a digit or X (representing a checksum of 10)
	if !unicode.IsDigit(rune(s[18])) && s[18] != 'X' {
		return false
	}
	return true
}

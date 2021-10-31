package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

var (
	connectionString = flag.String("connection", "", "connection string to database")
	filePath         = flag.String("path", "/etc/pgbouncer/userlist.txt", "path to userlist.txt file")
	excludeAccounts  = flag.String("exclude", "postgres,replicator,monitor", "exclude users from userlist.txt file")
)

func main() {
	flag.Parse()
	db, errOpen := sql.Open(`postgres`, *connectionString)
	if errOpen != nil {
		log.Fatalf("open connection: %s\n", errOpen)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if errGenerate := generateUserList(ctx, db, *filePath, strings.Split(*excludeAccounts, ",")); errGenerate != nil {
		log.Fatalf("generate userlist: %s\n", errGenerate)
	}
}

func generateUserList(ctx context.Context, db *sql.DB, path string, exclude []string) error {
	tmpConfigPath := path + ".tmp"
	tx, errTx := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if errTx != nil {
		return errTx
	}
	// nolint:errcheck
	defer tx.Commit()
	rows, errRows := tx.QueryContext(ctx, `
select distinct
    id.rolname,
    id.rolpassword
from pg_authid as id
    left join pg_catalog.pg_auth_members m on id.oid = m.member
    left join pg_catalog.pg_roles r on m.roleid = r.oid
where (r.rolname is null or not(r.rolname::text=ANY($1))) and id.rolpassword is not null
`, pq.Array(exclude))
	if errRows != nil {
		return errRows
	}
	// notlint:errcheck
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var username, password string
		if errScan := rows.Scan(&username, &password); errScan != nil {
			return errScan
		}
		lines = append(lines, fmt.Sprintf(`"%s" "%s"`, username, password))
	}
	if errRowsClose := rows.Err(); errRowsClose != nil {
		return errRowsClose
	}
	sort.Strings(lines)
	if errWrite := ioutil.WriteFile(tmpConfigPath, []byte(strings.Join(lines, "\n")+"\n"), 0600); errWrite != nil {
		return errWrite
	}
	// nolint:errcheck
	defer os.Remove(tmpConfigPath)
	if _, err := os.Stat(path); err == nil {
		currentMd5, errCurrentMd5 := calcMd5File(tmpConfigPath)
		if errCurrentMd5 != nil {
			return errCurrentMd5
		}
		oldMd5, errOldMd5 := calcMd5File(path)
		if errOldMd5 != nil {
			return errOldMd5
		}
		if currentMd5 == oldMd5 {
			log.Printf("[INFO] pgbouncer user list files doesn't have any changes, skipping update\n")
			return nil
		}
		if errBackup := copyFile(path,
			fmt.Sprintf("%s.backup-%d", path, time.Now().UTC().Unix())); errBackup != nil {
			return errBackup
		}
	}
	return os.Rename(tmpConfigPath, path)
}

func calcMd5File(filename string) (string, error) {
	filename = filepath.Clean(filename)
	// nolint:gosec
	fd, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	// nolint:errcheck,gosec
	defer fd.Close()
	// nolint:gosec
	hash := md5.New()
	if _, err := io.Copy(hash, fd); err != nil {
		return "", err
	}
	hashInBytes := hash.Sum(nil)[:16]
	return hex.EncodeToString(hashInBytes), nil
}

func copyFile(src, dst string) error {
	src, dst = filepath.Clean(src), filepath.Clean(dst)
	// nolint:gosec
	in, errOpen := os.Open(src)
	if errOpen != nil {
		return errOpen
	}
	// nolint:errcheck,gosec
	defer in.Close()

	out, errCreate := os.Create(dst)
	if errCreate != nil {
		return errCreate
	}
	// nolint:errcheck,gosec
	defer out.Close()

	_, errCopy := io.Copy(out, in)
	if errCopy != nil {
		return errCopy
	}
	return out.Close()
}

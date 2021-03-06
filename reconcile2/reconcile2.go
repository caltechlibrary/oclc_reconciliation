package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
	// Caltech Library Packages
	"github.com/caltechlibrary/datatools"
)

var (
	matchedIDs map[string]bool
)

type Record struct {
	MaterialType string
	MonoOrSerial string
	Date1        string
	Date2        string
	Form         string
	Tind         string
	OCLC         string
	ISBN         string
	ISSN         string
	Title        string
	SubTitle     string
	Author       string
	Publisher    string
	Year         string
	Pagination   string
	MatchedCount int
}

func (r *Record) Header() string {
	return `material type,mono or serial,date1,date2,form,tind,OCLC,ISBN,ISSN,title,subtitle,author,publisher,year,pagination,matched count`
}

func (r *Record) String() string {
	return fmt.Sprintf("%q,%q,%q,%q,%q,%q,%q,%q,%q,%q,%q,%q,%q,%q,%q,%d",
		r.MaterialType, r.MonoOrSerial, r.Date1, r.Date2, r.Form,
		r.Tind, r.OCLC, r.ISBN, r.ISSN, r.Title,
		r.SubTitle, r.Author, r.Publisher, r.Year,
		r.Pagination, r.MatchedCount)
}

func RowToRecord(columnNames, row []string) *Record {
	rec := new(Record)
	for colNo, cName := range columnNames {
		switch cName {
		case "material type":
			rec.MaterialType = row[colNo]
		case "mono or serial":
			rec.MonoOrSerial = row[colNo]
		case "date1":
			rec.Date1 = row[colNo]
		case "date2":
			rec.Date2 = row[colNo]
		case "form":
			rec.Form = row[colNo]
		case "tind":
			rec.Tind = row[colNo]
		case "oclc":
			rec.OCLC = row[colNo]
		case "isbn":
			rec.ISBN = row[colNo]
		case "issn":
			rec.ISSN = row[colNo]
		case "title":
			rec.Title = row[colNo]
		case "subtitle":
			rec.SubTitle = row[colNo]
		case "author":
			rec.Author = row[colNo]
		case "publisher":
			rec.Publisher = row[colNo]
		case "year":
			rec.Year = row[colNo]
		case "pagination":
			rec.Pagination = row[colNo]
		}
	}
	return rec
}

func mkTable(src []byte) ([][]string, error) {
	r := csv.NewReader(bytes.NewReader(src))
	table, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	return table, nil
}

func mkRecords(src []byte, columnNames []string) ([]*Record, error) {
	table, err := mkTable(src)
	if err != nil {
		return nil, err
	}
	records := []*Record{}
	for i, row := range table {
		//NOTE: We need to skip the header row
		if i > 0 {
			rec := RowToRecord(columnNames, row)
			records = append(records, rec)
		}
	}
	return records, nil
}

func countTrue(booleans ...bool) int {
	cnt := 0
	for _, val := range booleans {
		if val == true {
			cnt++
		}
	}
	return cnt
}

func Match(target, source *Record, withLevenshtein bool) bool {
	if withLevenshtein == true {
		// Finally try using the Levenshtein approximate match without case sensitivety
		if datatools.Levenshtein(target.Title, source.Title, 1, 1, 1, false) <= 1 &&
			countTrue((target.MaterialType == source.MaterialType), (target.MonoOrSerial == source.MonoOrSerial),
				(target.Date1 == source.Date1), (target.Date2 == source.Date2), (target.Form == source.Form),
				(target.ISBN == source.ISBN), (target.ISSN == source.ISSN), (target.Publisher == source.Publisher),
				(target.Year == source.Year)) > 5 {
			return true
		}
	} else {
		// Try simple unaltered string match
		if target.Title == source.Title &&
			countTrue((target.MaterialType == source.MaterialType), (target.MonoOrSerial == source.MonoOrSerial),
				(target.Date1 == source.Date1), (target.Date2 == source.Date2), (target.Form == source.Form),
				(target.ISBN == source.ISBN), (target.ISSN == source.ISSN), (target.Publisher == source.Publisher),
				(target.Year == source.Year)) > 5 {
			return true
		}

		// FIXME: Try comparing with stop words removed

		// Try simple match strings where we trim lead/trailing spaces
		if strings.TrimSpace(target.Title) == strings.TrimSpace(source.Title) &&
			countTrue((target.MaterialType == source.MaterialType), (target.MonoOrSerial == source.MonoOrSerial),
				(target.Date1 == source.Date1), (target.Date2 == source.Date2), (target.Form == source.Form),
				(target.ISBN == source.ISBN), (target.ISSN == source.ISSN), (target.Publisher == source.Publisher),
				(target.Year == source.Year)) > 5 {
			return true
		}
	}
	return false
}

func Merge(target, source *Record) *Record {
	if source.Tind == "" {
		source.Tind = target.Tind
	}
	if source.OCLC == "" {
		source.OCLC = target.OCLC
	}
	return source
}

func Scan(target *Record, sources []*Record, withLevenshtein bool) string {
	matched := []*Record{}
	for _, source := range sources {
		if Match(target, source, withLevenshtein) == true {
			matched = append(matched, Merge(target, source))
		}
	}
	mCnt := len(matched)
	if mCnt > 0 {
		s := []string{}
		for _, rec := range matched {
			rec.MatchedCount = mCnt
			s = append(s, rec.String())
		}
		//log.Printf("Found %d matches for %q", mCnt, target.Title)
		return strings.Join(s, "\n")
	}
	return ""
}

func main() {
	var (
		oclcColumns = []string{
			"material type", // 0
			"mono or serial",
			"date1",
			"date2",
			"form", // 4
			"isbn",
			"issn",
			"oclc", // 7
			"title",
			"subtitle",
			"author",
			"publisher",
			"year",
			"pagination",
		}

		tindColumns = []string{
			"material type", // 0
			"mono or serial",
			"date1",
			"date2",
			"form", // 4
			"tind",
			"oclc", // 6
			"isbn",
			"issn",
			"title",
			"subtitle",
			"author",
			"publisher",
			"year",
			"pagination",
		}
	)

	percentage := func(x, y int) string {
		if y != 0 {
			f := (float64(x) / float64(y)) * 100.0
			return fmt.Sprintf("%3.1f%%", f)
		}
		return "0%"
	}

	startT := time.Now()
	// Read in the OCLC IDs we already have tested, in reconcile
	matchedIDsSrc, err := ioutil.ReadFile("matched-ids.csv")
	if err != nil {
		log.Fatal("Can't read matched-ids.csv, %s", err)
	}
	matchedIDs = make(map[string]bool)
	for _, line := range bytes.Split(matchedIDsSrc, []byte("\n")) {
		val := fmt.Sprintf("%s", bytes.TrimSpace(line))
		if len(val) > 0 {
			matchedIDs[val] = true
		}
	}
	previouslyProcessedCnt := len(matchedIDs)
	log.Printf("Previouslty processed IDs %d", previouslyProcessedCnt)

	oclcSrc, err := ioutil.ReadFile("data/rerun-oclc-all.csv")
	if err != nil {
		log.Fatal("Can't read data/rerun-oclc-all.csv, %s", err)
	}
	log.Printf("Read in data/rerun-oclc-all.csv, running time %s", time.Now().Sub(startT))
	tindSrc, err := ioutil.ReadFile("data/rerun-tind-all.csv")
	if err != nil {
		log.Fatal("Can't read data/rerun-tind-all.csv, %s", err)
	}
	log.Printf("Read in data/rerun-tind-all.csv, running time %s", time.Now().Sub(startT))

	oclc, err := mkRecords(oclcSrc, oclcColumns)
	if err != nil {
		log.Fatal("Can't decode oclc CSV, %s", err)
	}
	oclcCnt := len(oclc)
	log.Printf("oclc rows: %d, running time %s", oclcCnt, time.Now().Sub(startT))

	tind, err := mkRecords(tindSrc, tindColumns)
	if err != nil {
		log.Fatal("Can't decode tind CSV, %s", err)
	}
	log.Printf("tind rows: %d, running time %s", len(tind), time.Now().Sub(startT))
	filterT := time.Now()
	matchedCnt := 0
	unmatchedCnt := 0
	rec := new(Record)
	// First pass will be of rows using Scan, the unmatched rows will then get scanned using separage Scan2
	log.Printf("Running with simple title matching running time %s", time.Now().Sub(startT))
	fmt.Fprintln(os.Stdout, rec.Header())
	for i, rec := range oclc {
		if _, ok := matchedIDs[rec.OCLC]; ok == true {
			//log.Printf("Skipping %s", rec.OCLC)
			oclcCnt--
		} else {
			if s := Scan(rec, tind, false); s != "" {
				fmt.Fprintf(os.Stdout, "%s\n", s)
				matchedCnt++
			} else {
				rec.MatchedCount = 0
				fmt.Fprintf(os.Stdout, "%s\n", s)
				unmatchedCnt++
			}
		}
		if (i % 100) == 0 {
			t := time.Now()
			p := matchedCnt + unmatchedCnt
			log.Printf("%d matched, %d unmatched", matchedCnt, unmatchedCnt)
			log.Printf("%d/%d (%s) rows processed in OCLC CSV, batch time %s, running time %s",
				p, oclcCnt, percentage(p, oclcCnt), t.Sub(filterT), t.Sub(startT))
			filterT = t
		}
	}
	log.Printf("Running time %s", time.Now().Sub(startT))
}

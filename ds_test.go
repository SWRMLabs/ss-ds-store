package ds

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	store "github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	logger "github.com/ipfs/go-log/v2"
)

type successStruct struct {
	Namespace string
	Id        string
	FileName  string
	CreatedAt int64
	UpdatedAt int64
}

func (t *successStruct) GetNamespace() string { return t.Namespace }

func (t *successStruct) GetId() string { return t.Id }

func (t *successStruct) Marshal() ([]byte, error) { return json.Marshal(t) }

func (t *successStruct) Unmarshal(val []byte) error { return json.Unmarshal(val, t) }

func (t *successStruct) SetCreated(unixTime int64) { t.CreatedAt = unixTime }

func (t *successStruct) SetUpdated(unixTime int64) { t.UpdatedAt = unixTime }

func (t *successStruct) GetCreated() int64 { return t.CreatedAt }

func (t *successStruct) GetUpdated() int64 { return t.UpdatedAt }

var cnf DSConfig

var dsHndlr store.Store

func TestMain(m *testing.M) {
	logger.SetLogLevel("*", "Debug")
	dsPath := "/tmp/dataStore"

	os.RemoveAll(dsPath)

	ds, err := badger.NewDatastore(dsPath, &badger.DefaultOptions)
	if err != nil {
		panic("Failed to initialize ds")
	}
	cnf = DSConfig{
		DS: ds,
	}

	code := m.Run()
	os.Exit(code)
}

func TestNewCreation(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}

	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		FileName:  "MyTestFile.txt",
	}

	err = dsHndlr.Create(&d)

	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestNewRead(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}

	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}
	err = dsHndlr.Read(&d)
	if err != nil {
		t.Fatal(err.Error())
	}

	if d.FileName != "MyTestFile.txt" {
		t.Fatal("filename doesn't match")
	}
}

func TestNewUpdate(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Fatal(" store init failed")
	}
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}
	err = dsHndlr.Read(&d)
	if err != nil {
		t.Fatal(err.Error())
	}
	privTime := d.GetUpdated()
	<-time.After(time.Second * 1)
	d.FileName = "MyUpdatedFile.txt"

	err = dsHndlr.Update(&d)
	if err != nil {
		t.Fatal(err.Error())
	}
	newTime := d.GetUpdated()

	if privTime+1 != newTime {
		t.Fatal("updatedAt didn't work properly")
	}
}

func TestNewDelete(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}

	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}

	err = dsHndlr.Delete(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = dsHndlr.Read(&d)
	if err != datastore.ErrNotFound {
		t.Fatal("error should be of type ErrNotFound")
	}
}

func TestSortNaturalList(t *testing.T) {
	// Create some dummies with StreamSpace namespace
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
			Id:        uuid.New().String(),
		}
		err := dsHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		//<-time.After(time.Second * 1)
	}

	//Create some dummies with Other namespace
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "Other",
			Id:        uuid.New().String(),
		}
		err := dsHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		//<-time.After(time.Second * 1)
	}

	var sort store.Sort

	sort = 0

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	ds := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		ds = append(ds, &d)
	}

	count, err := dsHndlr.List(ds, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if count == 0 {
		t.Fatalf("count should not be zero")
	}

	for i := 0; i < count; i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestSortCreatedAscList(t *testing.T) {
	// Create some dummies with StreamSpace namespace
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	for i := 0; i < 10; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
			Id:        uuid.New().String(),
		}
		err := dsHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		<-time.After(time.Second * 1)
	}

	//Create some dummies with Other namespace
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "Other",
			Id:        uuid.New().String(),
		}
		err := dsHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		<-time.After(time.Second * 1)
	}

	opts := store.ListOpt{
		Page:  0,
		Limit: 10,
		Sort:  store.SortCreatedAsc,
	}

	ds := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		ds = append(ds, &d)
	}

	count, err := dsHndlr.List(ds, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if count == 0 {
		t.Fatalf("count should not be zero")
	}
	for i := 0; i < count; i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestSortCreatedDscList(t *testing.T) {
	// Create some dummies with StreamSpace namespace
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	for i := 0; i < 10; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
			Id:        uuid.New().String(),
		}
		err := dsHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		<-time.After(time.Second * 1)
	}

	//Create some dummies with Other namespace
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "Other",
			Id:        uuid.New().String(),
		}
		err := dsHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		<-time.After(time.Second * 1)
	}

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  store.SortCreatedDesc,
	}

	ds := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		ds = append(ds, &d)
	}

	count, err := dsHndlr.List(ds, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if count == 0 {
		t.Fatalf("count should not be zero")
	}
	for i := 0; i < count; i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestHardTestingASC(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}

	for i := 0; i < 30; i++ {
		d := successStruct{
			Namespace: "hardtest",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}
	for i := 0; i < 15; i++ {
		d := successStruct{
			Namespace: "other",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 1)
	}
	for i := 0; i < 6; i++ {
		opts := store.ListOpt{
			Page:  int64(i),
			Limit: 5,
			Sort:  store.SortCreatedAsc,
		}
		ds := store.Items{}
		for i := 0; int64(i) < opts.Limit; i++ {
			d := successStruct{
				Namespace: "hardtest",
			}
			ds = append(ds, &d)
		}
		count, err := dsHndlr.List(ds, opts)
		if err != nil {
			t.Error(err)
		}
		if count == 0 {
			t.Errorf("Count can't be zero")
		}
		for i := 0; i < count; i++ {
			if ds[i].GetNamespace() != "hardtest" {
				t.Errorf("Name of %vth element doesn't match", i)
			}
		}
	}
}

func TestHardTestingDESC(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}

	for i := 0; i < 25; i++ {
		d := successStruct{
			Namespace: "Noval-Testing",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}
	for i := 0; i < 15; i++ {
		d := successStruct{
			Namespace: "other",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 1)
	}
	for i := 0; i < 5; i++ {
		opts := store.ListOpt{
			Page:  int64(i),
			Limit: 5,
			Sort:  store.SortCreatedDesc,
		}
		ds := store.Items{}
		for i := 0; int64(i) < opts.Limit; i++ {
			d := successStruct{
				Namespace: "Noval-Testing",
			}
			ds = append(ds, &d)
		}
		count, err := dsHndlr.List(ds, opts)
		if err != nil {
			t.Error(err)
		}
		if count == 0 {
			t.Errorf("Count can't be zero")
		}
		for i := 0; i < count; i++ {
			if ds[i].GetNamespace() != "Noval-Testing" {
				t.Errorf("Name of %vth element doesn't match", i)
			}
		}
		<-time.After(time.Second * 2)
	}
}

func TestUpadationCheckASC(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	var id []string
	for i := 0; i < 25; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        uuid.New().String(),
		}
		id = append(id, d.Id)
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 15; i++ {
		d := successStruct{
			Namespace: "other",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 25; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        id[i],
		}
		err := dsHndlr.Read(&d)
		if err != nil {
			t.Errorf("Unable to read %s", err.Error())
		}
		d.FileName = "Update file"
		err = dsHndlr.Update(&d)
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 5; i++ {
		opts := store.ListOpt{
			Page:  int64(i),
			Limit: 5,
			Sort:  store.SortUpdatedAsc,
		}
		ds := store.Items{}
		for i := 0; int64(i) < opts.Limit; i++ {
			d := successStruct{
				Namespace: "Update-Testing",
			}
			ds = append(ds, &d)
		}

		count, err := dsHndlr.List(ds, opts)
		if err != nil {
			t.Error(err)
		}

		if count == 0 {
			t.Errorf("Count can't be zero")
		}

		for i := 0; i < count; i++ {
			if ds[i].GetNamespace() != "Update-Testing" {
				t.Errorf("Name of %vth element doesn't match", i)
			}
		}
	}
}

func TestUpadationCheckDESC(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	var id []string
	for i := 0; i < 25; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        uuid.New().String(),
		}
		id = append(id, d.Id)
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 15; i++ {
		d := successStruct{
			Namespace: "other",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 25; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        id[i],
		}
		err := dsHndlr.Read(&d)
		if err != nil {
			t.Errorf("Unable to read %s", err.Error())
		}
		d.FileName = "Update file"
		err = dsHndlr.Update(&d)
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 5; i++ {
		opts := store.ListOpt{
			Page:  int64(i),
			Limit: 5,
			Sort:  store.SortUpdatedDesc,
		}
		ds := store.Items{}
		for i := 0; int64(i) < opts.Limit; i++ {
			d := successStruct{
				Namespace: "Update-Testing",
			}
			ds = append(ds, &d)
		}

		count, err := dsHndlr.List(ds, opts)
		if err != nil {
			t.Error(err)
		}

		if count == 0 {
			t.Errorf("Count can't be zero")
		}

		for i := 0; i < count; i++ {
			if ds[i].GetNamespace() != "Update-Testing" {
				t.Errorf("Name of %vth element doesn't match", i)
			}
		}
	}
}

func TestUpdateASC(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	var id []string
	for i := 0; i < 40; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        uuid.New().String(),
		}
		id = append(id, d.Id)
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 15; i++ {
		d := successStruct{
			Namespace: "other",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 1)
	}

	for i := 0; i < 40; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        id[i],
		}
		err := dsHndlr.Read(&d)
		if err != nil {
			t.Errorf("Unable to read %s", err.Error())
		}
		d.FileName = "Update file"
		err = dsHndlr.Update(&d)
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 2; i++ {
		opts := store.ListOpt{
			Page:  int64(i),
			Limit: 20,
			Sort:  store.SortUpdatedAsc,
		}
		ds := store.Items{}
		for i := 0; int64(i) < opts.Limit; i++ {
			d := successStruct{
				Namespace: "Update-Testing",
			}
			ds = append(ds, &d)
		}

		count, err := dsHndlr.List(ds, opts)
		if err != nil {
			t.Error(err)
		}

		if count == 0 {
			t.Errorf("Count can't be zero")
		}

		for i := 0; i < count; i++ {
			if ds[i].GetNamespace() != "Update-Testing" {
				t.Errorf("Name of %vth element doesn't match", i)
			}
		}
	}
}

func TestUpdateDESC(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Errorf(" store init failed %s", err.Error())
	}
	var id []string
	for i := 0; i < 40; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        uuid.New().String(),
		}
		id = append(id, d.Id)
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 15; i++ {
		d := successStruct{
			Namespace: "other",
			Id:        uuid.New().String(),
		}
		err = dsHndlr.Create(&d)
		if err != nil {
			t.Errorf("Unable to store data %s", err.Error())
		}
		<-time.After(time.Second * 1)
	}

	for i := 0; i < 40; i++ {
		d := successStruct{
			Namespace: "Update-Testing",
			Id:        id[i],
		}
		err := dsHndlr.Read(&d)
		if err != nil {
			t.Errorf("Unable to read %s", err.Error())
		}
		d.FileName = "Update file"
		err = dsHndlr.Update(&d)
		<-time.After(time.Second * 2)
	}

	for i := 0; i < 2; i++ {
		opts := store.ListOpt{
			Page:  int64(i),
			Limit: 20,
			Sort:  store.SortUpdatedDesc,
		}
		ds := store.Items{}
		for i := 0; int64(i) < opts.Limit; i++ {
			d := successStruct{
				Namespace: "Update-Testing",
			}
			ds = append(ds, &d)
		}

		count, err := dsHndlr.List(ds, opts)
		if err != nil {
			t.Error(err)
		}

		if count == 0 {
			t.Errorf("Count can't be zero")
		}

		for i := 0; i < count; i++ {
			if ds[i].GetNamespace() != "Update-Testing" {
				t.Errorf("Name of %vth element doesn't match", i)
			}
		}
	}
}

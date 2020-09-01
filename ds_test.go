package ds

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
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

type streamspaceFactory struct {}

func (f streamspaceFactory) Factory() store.SerializedItem {
	return &successStruct{
		Namespace: "StreamSpace",
	}
}

type otherFactory struct {}

func (f otherFactory) Factory() store.SerializedItem {
	return &successStruct{
		Namespace: "Other",
	}
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

func TestNewStore(t *testing.T) {
	var err error
	dsHndlr, err = NewDataStore(&cnf)
	if err != nil {
		t.Fatal(" store init failed")
	}
}

func TestNewCreation(t *testing.T) {
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		FileName:  "MyTestFile.txt",
	}

	err := dsHndlr.Create(&d)

	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestNewRead(t *testing.T) {
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}
	err := dsHndlr.Read(&d)
	if err != nil {
		t.Fatal(err.Error())
	}

	if d.FileName != "MyTestFile.txt" {
		t.Fatal("filename doesn't match")
	}
}

func TestNewUpdate(t *testing.T) {
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}
	err := dsHndlr.Read(&d)
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
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}

	err := dsHndlr.Delete(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = dsHndlr.Read(&d)
	if err != store.ErrRecordNotFound {
		t.Fatal("error should be of type ErrNotFound")
	}
}

func TestSortNaturalList(t *testing.T) {
	// Create some dummies with StreamSpace namespace
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

	ds, err := dsHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(ds) == 0 {
		t.Fatalf("count should not be zero")
	}

	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

type filterFileName struct {
	fileName string
}

func (f filterFileName) Factory() store.SerializedItem {
	return &successStruct{}
}

func (f filterFileName) Compare(i store.SerializedItem) bool {
	st, ok := i.(*successStruct)
	if !ok {
		return false
	}
	return st.FileName == f.fileName
}

func TestSortNaturalListWithFilter(t *testing.T) {
	// Create some dummies with StreamSpace namespace
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
			Id:        uuid.New().String(),
			FileName: fmt.Sprintf("File%d", i),
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
		Filter: &filterFileName{fileName: "File1"},
	}

	ds, err := dsHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(ds) == 0 {
		t.Fatalf("count should not be zero")
	}

	if len(ds) != 1 {
		t.Fatalf("count should be 1")
	}

	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestSortCreatedAscList(t *testing.T) {
	// Create some dummies with StreamSpace namespace
	for i := 0; i < 5; i++ {
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

	ds, err := dsHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(ds) == 0 {
		t.Fatalf("count should not be zero")
	}
	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestSortCreatedDscList(t *testing.T) {
	// Create some dummies with StreamSpace namespace
	for i := 0; i < 5; i++ {
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

	ds, err := dsHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(ds) == 0 {
		t.Fatalf("count should not be zero")
	}
	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

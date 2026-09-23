package journal_test

import (
 "bytes"
 "context"
 "io"
 "os"
 "path/filepath"
 "reflect"
 "fmt"
 "testing"
 "time"
 journal "github.com/Laisky/go-journal"
)

func TestUserSegmentCreationNeverOpensAnExistingFile(t *testing.T) {
 name:=filepath.Join(t.TempDir(),"owned.buf")
 original:=[]byte("already owned by another operation")
 if err:=os.WriteFile(name,original,0600);err!=nil {t.Fatal(err)}
 fp,err:=journal.OpenBufFile(name,0)
 if fp!=nil {fp.Close()}
 if err==nil {t.Error("segment creation opened an existing file for overwrite")}
 got,e:=os.ReadFile(name);if e!=nil||!bytes.Equal(got,original) {t.Fatal("existing bytes changed")}
}

func TestUserFilenameErrorsDoNotPanic(t *testing.T) {
 for _,name:=range []string{"", ".buf", "x.ids", "12345678.buf", "20260101_99999999.buf", "../20260101_00000001.buf", "20260101_00000001.bufXgz"} {t.Run(name,func(t *testing.T){
  defer func(){if p:=recover();p!=nil {t.Errorf("filename input panicked: %v",p)}}()
  if _,err:=journal.GenerateNewBufFName(time.Date(2026,1,1,0,0,0,0,time.UTC),name);err==nil {t.Error("invalid/exhausted filename accepted")}
 })}
}

func TestUserInvalidPayloadDoesNotPoisonLaterAcceptedData(t *testing.T) {
 for _,gz:=range []bool{false,true} {t.Run(fmt.Sprint(gz),func(t *testing.T){
  dir:=t.TempDir();j:=userJournal(t,dir,gz);userPut(t,j,1);userSync(t,j)
  if err:=j.WriteData(&journal.Data{ID:2,Data:map[string]interface{}{"unsupported":make(chan int)}});err==nil {t.Fatal("unsupported payload accepted")}
  userPut(t,j,3);userSync(t,j);j.Close();j=userJournal(t,dir,gz)
  if got:=userReplay(t,j);!reflect.DeepEqual(got,map[int64]string{1:"1",3:"3"}) {t.Fatalf("invalid payload poisoned successful records: %v",got)}
 })}
}

func TestUserCleanupFailureRemainsVisible(t *testing.T) {
 dir:=t.TempDir();j:=userJournal(t,dir,false);userPut(t,j,1);userSync(t,j)
 old:=userNames(t,dir,".buf")[0]
 if err:=j.Rotate(context.Background());err!=nil {t.Fatal(err)}
 if !j.LockLegacy() {t.Fatal("lock")}
 d:=new(journal.Data);if err:=j.LoadLegacyBuf(d);err!=nil {t.Fatal(err)}
 if err:=j.WriteData(d);err!=nil {t.Fatal(err)}
 // The open source descriptor remains readable, but unlink fails on a nonempty directory.
 if err:=os.Rename(old,old+".held");err!=nil {t.Fatal(err)}
 if err:=os.Mkdir(old,0700);err!=nil {t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(old,"busy"),[]byte("busy"),0600);err!=nil {t.Fatal(err)}
 err:=j.LoadLegacyBuf(new(journal.Data));j.UnLockLegacy()
 if err==nil||err==io.EOF {t.Errorf("failed cleanup reported a successful EOF: %v",err)}
 if err=os.RemoveAll(old);err!=nil {t.Fatal(err)}
 if err=os.Rename(old+".held",old);err!=nil {t.Fatal(err)}
 if got:=userReplay(t,j);!reflect.DeepEqual(got,map[int64]string{1:"1"}) {t.Fatalf("cleanup retry lost snapshot: %v",got)}
}

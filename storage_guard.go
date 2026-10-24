package journal

import "errors"

// storageGrant is the sole callback boundary for guarded journal mutations.
// The caller must hold the journal read or write lock before entering this
// method. It must release the grant before unlocking the journal. A guard must
// not reenter journal methods or acquire locks in the opposite order.
func (j *Journal) storageGrant(kind string, bytes int64) (func(), error) {
	if j == nil || j.option == nil || j.storageGuard == nil {
		return func() {}, nil
	}
	release, err := j.storageGuard(kind, bytes)
	if err != nil {
		return nil, err
	}
	if release == nil {
		return nil, errors.New("journal: successful storage guard grant has nil release")
	}
	return release, nil
}

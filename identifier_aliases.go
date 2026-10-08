package journal

// WriteID acknowledges a record. It is the idiomatic spelling of WriteId;
// both methods preserve the same validation, lifecycle and durability contract.
func (j *Journal) WriteID(id int64) error { return j.WriteId(id) }

// LoadMaxID is the idiomatic spelling of LoadMaxId.
func (j *Journal) LoadMaxID() (int64, error) { return j.LoadMaxId() }

// LoadMaxID is the idiomatic spelling of LoadMaxId.
func (l *LegacyLoader) LoadMaxID() (int64, error) { return l.LoadMaxId() }

// LoadMaxID is the idiomatic spelling of LoadMaxId.
func (dec *IdsDecoder) LoadMaxID() (int64, error) { return dec.LoadMaxId() }

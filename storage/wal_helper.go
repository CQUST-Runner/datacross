package storage

type WalHelper struct {
	w          *Wal
	filename   string
	l          LogFormat
	writeCount int
	count      int
}

func (w *WalHelper) Init(filename string, l LogFormat, writeCount int) {
	w.filename = filename
	w.l = l
	w.writeCount = writeCount
	w.count = 0
}

func (w *WalHelper) Close() {
	if w.w != nil {
		err := w.w.Close()
		if err != nil {
			logger.Error("close wal failed[%v]", err)
			w.w = nil
		}
		w.w = nil
	}
}

func (w *WalHelper) check() {
	if w.count >= w.writeCount {
		if w.w != nil {
			if err := w.w.Flush(); err != nil {
				logger.Error("flush wal failed[%v]", err)
				return
			}
			if err := w.w.Close(); err != nil {
				logger.Error("close wal failed[%v]", err)
				return
			}
			w.w = nil
		}
		w.count = 0
	}
}

func (w *WalHelper) getW() (*Wal, error) {
	if w.w == nil {
		wal := Wal{}
		err := wal.Init(w.filename, w.l, false)
		if err != nil {
			return nil, err
		}
		w.w = &wal
		return w.w, nil
	}
	return w.w, nil
}

func (w *WalHelper) Append(logOp ...*LogOperation) (string, int64, error) {
	wal, err := w.getW()
	if err != nil {
		return "", 0, err
	}
	gid, num, err := wal.Append(logOp...)
	if err != nil {
		return "", 0, err
	}

	w.count++
	w.check()

	return gid, num, nil
}

func (w *WalHelper) Offset() (int64, error) {
	wal, err := w.getW()
	if err != nil {
		return 0, err
	}
	return wal.Offset(), nil
}

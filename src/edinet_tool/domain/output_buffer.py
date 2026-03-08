class OutputBuffer:
    def __init__(self):
        self._data = {}
        self._src = {}
        self._collisions = []

    def put(self, key: str, value, src_label: str):
        if value is None or value == "":
            return

        prio = {
            "half_final": 3,
            "file1_half": 3,
            "file2_annual": 2,
            "file3_annual": 1,
        }
        new_p = prio.get(src_label, 0)

        if key in self._data:
            old_src = self._src.get(key, "?")
            old_p = prio.get(old_src, 0)

            if old_src in ("half_final", "file1_half") and src_label in ("file2_annual", "file3_annual"):
                self._collisions.append((key, old_src, src_label))
                return

            if new_p < old_p:
                self._collisions.append((key, old_src, src_label))
                return

            self._collisions.append((key, old_src, src_label))

        self._data[key] = value
        self._src[key] = src_label

    def to_dict(self):
        return dict(self._data)

    def collisions(self):
        return list(self._collisions)

    def winner_of(self, key: str):
        return self._src.get(key, "?")
    
    def has(self, key: str) -> bool:
        return key in self._data

    def pop(self, key: str):
        if key in self._data:
            self._data.pop(key, None)
            self._src.pop(key, None)

    def __len__(self):
        return len(self._data)

    def __bool__(self):
        return bool(self._data)
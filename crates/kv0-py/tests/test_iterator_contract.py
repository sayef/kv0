"""Exhaustive checks of the iterator contract.

Two consumption styles must work on the SAME iterator object:
* ``get()`` peeks (raises ``ValueError`` past end) and ``next(it)`` (``__next__``)
  raises ``StopIteration`` past end.
* the ``.next()`` *method* returns the current item then advances (``None`` past end).
"""

import os
import tempfile
import unittest

import kv0


class TestIteratorContract(unittest.TestCase):
    def setUp(self):
        self.loc = tempfile.mkdtemp()
        self.db = kv0.DB(os.path.join(self.loc, "db"), kv0.Options(create_if_missing=True))
        for k in (b"a", b"b", b"c", b"d"):
            self.db.put(k, k.upper())

    def test_get_peeks_without_advancing(self):
        it = self.db.iteritems()
        it.seek(b"a")
        self.assertEqual(it.get(), (b"a", b"A"))
        self.assertEqual(it.get(), (b"a", b"A"))  # idempotent peek

    def test_method_next_returns_current_then_advances(self):
        it = self.db.iteritems()
        it.seek(b"a")
        self.assertEqual(it.next(), (b"a", b"A"))
        self.assertEqual(it.next(), (b"b", b"B"))

    def test_method_next_returns_none_at_end(self):
        it = self.db.iteritems()
        it.seek(b"z")
        self.assertIsNone(it.next())

    def test_dunder_next_raises_stopiteration(self):
        it = self.db.iteritems()
        it.seek(b"d")
        self.assertEqual(next(it), (b"d", b"D"))
        with self.assertRaises(StopIteration):
            next(it)

    def test_get_raises_valueerror_past_end(self):
        it = self.db.iteritems()
        it.seek(b"z")
        with self.assertRaises(ValueError):
            it.get()

    def test_seek_for_prev_then_get(self):
        it = self.db.iteritems()
        it.seek_for_prev(b"c")
        self.assertEqual(it.get(), (b"c", b"C"))
        it.seek_for_prev(b"bb")  # between b and c -> b
        self.assertEqual(it.get(), (b"b", b"B"))

    def test_full_forward_scan(self):
        it = self.db.iteritems()
        it.seek_to_first()
        self.assertEqual(list(it), [(b"a", b"A"), (b"b", b"B"), (b"c", b"C"), (b"d", b"D")])

    def test_deleted_keys_are_skipped(self):
        self.db.delete(b"b")
        it = self.db.iteritems()
        it.seek_to_first()
        self.assertEqual([k for k, _ in it], [b"a", b"c", b"d"])

    def tearDown(self):
        self.db.close()


if __name__ == "__main__":
    unittest.main()

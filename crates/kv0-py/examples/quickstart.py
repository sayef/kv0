"""Key-value quickstart: the ordered key-value API on a local directory.

The same code works on object storage: pass a URI such as ``s3://bucket/prefix/db`` and the
credentials or endpoint as ``storage_options``.
"""

import tempfile

import kv0

path = tempfile.mkdtemp() + "/example_db"
db = kv0.DB(path, kv0.Options(create_if_missing=True), read_only=False)

batch = kv0.WriteBatch()
batch.put(b"key_1", b"value_1")
batch.put(b"key_2", b"value_2")
db.write(batch)

print(db.get(b"key_1"))  # b'value_1'

it = db.iteritems()
it.seek(b"key_")
while (item := it.next()) is not None:
    print(item)

db.flush()  # commit buffered writes as one Lance version
print("version", db.version)
db.close()

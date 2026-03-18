"""Deterministic unit tests for model serialization invariants (T017)."""

from packed_kv_cosmos.model import (
    DOC_TYPE_PAGE,
    DOC_TYPE_ROOT,
    BucketPageDocument,
    BucketRootDocument,
)


class TestBucketRootDocumentSerialization:
    """Tests for BucketRootDocument.to_dict / from_dict round-trip."""

    def _make_root(self, **overrides) -> BucketRootDocument:
        defaults = dict(
            id="ns:abcd",
            pk="ns:abcd",
            schema_version=1,
            bucket_id="abcd",
            entries={"e1": "dGVzdA=="},
            tail_page=None,
            page_count=0,
            updated_at="2026-01-01T00:00:00Z",
            etag='"some-etag"',
        )
        defaults.update(overrides)
        return BucketRootDocument(**defaults)

    def test_round_trip(self) -> None:
        root = self._make_root()
        d = root.to_dict()
        restored = BucketRootDocument.from_dict({**d, "_etag": root.etag})
        assert restored.id == root.id
        assert restored.pk == root.pk
        assert restored.schema_version == root.schema_version
        assert restored.bucket_id == root.bucket_id
        assert restored.entries == root.entries
        assert restored.tail_page == root.tail_page
        assert restored.page_count == root.page_count

    def test_doc_type_is_root(self) -> None:
        d = self._make_root().to_dict()
        assert d["docType"] == DOC_TYPE_ROOT

    def test_entries_is_plain_dict(self) -> None:
        """Entries map must serialize as a plain JSON object."""
        root = self._make_root(entries={"a": "MQ==", "b": "Mg=="})
        d = root.to_dict()
        assert isinstance(d["entries"], dict)
        assert d["entries"] == {"a": "MQ==", "b": "Mg=="}

    def test_entries_keys_are_entry_ids(self) -> None:
        """Every key in the entries map must be the entry-id string."""
        root = self._make_root(entries={"abc123": "val"})
        d = root.to_dict()
        assert "abc123" in d["entries"]

    def test_etag_not_in_body(self) -> None:
        """The ETag is metadata, not part of the serialized body."""
        d = self._make_root(etag='"x"').to_dict()
        assert "_etag" not in d
        assert "etag" not in d

    def test_from_dict_reads_etag(self) -> None:
        d = self._make_root().to_dict()
        d["_etag"] = '"etag-value"'
        restored = BucketRootDocument.from_dict(d)
        assert restored.etag == '"etag-value"'

    def test_empty_entries(self) -> None:
        root = self._make_root(entries={})
        d = root.to_dict()
        assert d["entries"] == {}
        restored = BucketRootDocument.from_dict(d)
        assert restored.entries == {}


class TestBucketPageDocumentSerialization:
    """Tests for BucketPageDocument.to_dict / from_dict round-trip."""

    def _make_page(self, **overrides) -> BucketPageDocument:
        defaults = dict(
            id="ns:abcd:p0",
            pk="ns:abcd",
            schema_version=1,
            bucket_id="abcd",
            page_index=0,
            entries={"e2": "cGFnZQ=="},
            next_page=None,
            updated_at="2026-01-01T00:00:00Z",
            etag='"page-etag"',
        )
        defaults.update(overrides)
        return BucketPageDocument(**defaults)

    def test_round_trip(self) -> None:
        page = self._make_page()
        d = page.to_dict()
        restored = BucketPageDocument.from_dict({**d, "_etag": page.etag})
        assert restored.id == page.id
        assert restored.pk == page.pk
        assert restored.page_index == page.page_index
        assert restored.entries == page.entries

    def test_doc_type_is_page(self) -> None:
        d = self._make_page().to_dict()
        assert d["docType"] == DOC_TYPE_PAGE

    def test_page_index_present(self) -> None:
        d = self._make_page(page_index=5).to_dict()
        assert d["pageIndex"] == 5

    def test_next_page_field(self) -> None:
        d = self._make_page(next_page="ns:abcd:p1").to_dict()
        assert d["nextPage"] == "ns:abcd:p1"

    def test_etag_not_in_body(self) -> None:
        d = self._make_page().to_dict()
        assert "_etag" not in d

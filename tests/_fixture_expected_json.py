from __future__ import annotations


def sample_fixture_documents() -> list[dict[str, object]]:
    return [
        {
            "child": {"value": "child-1"},
            "id": 1,
            "items": [
                {
                    "amount": 7,
                    "enabled": True,
                    "label": "A",
                    "nested": {
                        "active": True,
                        "items": [{"value": "na-1"}, {"value": "na-2"}],
                        "note": "nested-a",
                        "pick": 1,
                        "score": 11,
                    },
                    "optional": "x",
                    "value": "first",
                },
                {
                    "enabled": False,
                    "label": "B",
                    "nested": {
                        "active": False,
                        "items": [{"value": "nb-1"}],
                        "note": "nested-b",
                        "pick": 0,
                        "score": 12,
                    },
                    "optional": None,
                    "value": "second",
                },
            ],
            "meta": {
                "flag": True,
                "info": {"note": "deep"},
                "items": [{"value": "m1"}, {"value": "m2"}],
            },
            "name": "alpha",
            "note": "x",
            "tags": ["red", "blue"],
            "value": 42,
        },
        {
            "id": 2,
            "items": [{"amount": 5, "label": "C", "value": "only"}],
            "meta": {"flag": False, "items": [{"value": "m3"}]},
            "name": "beta",
            "note": None,
            "tags": ["green"],
            "value": "43",
        },
        {
            "child": None,
            "id": 3,
            "name": "gamma",
            "value": None,
        },
    ]


def deepdoc_fixture_documents() -> list[dict[str, object]]:
    return [
        {
            "chain": {
                "next": {
                    "next": {
                        "next": {
                            "next": {
                                "next": {
                                    "next": {
                                        "next": {
                                            "entries": [
                                                {"extras": ["x0", "x1"], "kind": "root", "value": "e0"},
                                                {"kind": "mid", "value": "e1"},
                                                {"extras": ["tail-extra"], "kind": "tail", "value": "e2"},
                                            ],
                                            "leaf_note": "bottom",
                                            "reading": 100,
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "doc_id": 101,
            "metrics": [10, 20, 30],
            "profile": {"nickname": None, "prefs": {"theme": "dark"}},
            "tags": ["alpha", "beta", "gamma"],
            "title": "deep-alpha",
        },
        {
            "chain": {
                "next": {
                    "next": {
                        "next": {
                            "next": {
                                "next": {
                                    "next": {
                                        "next": {
                                            "entries": [{"extras": ["solo-extra"], "kind": "solo", "value": "other"}],
                                            "leaf_note": None,
                                            "reading": "101",
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "doc_id": 102,
            "metrics": [7],
            "profile": {"prefs": {"theme": None}},
            "tags": ["delta"],
            "title": "deep-beta",
        },
        {
            "doc_id": 103,
            "title": "deep-gamma",
        },
    ]


def bigdoc_fixture_documents() -> list[dict[str, object]]:
    return [
        {
            "big": 123456789012345678901234567890123456,
            "label": "huge",
        },
        {
            "big": None,
            "label": "null",
        },
        {
            "big": 7,
            "label": "small",
        },
    ]

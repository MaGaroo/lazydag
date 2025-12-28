import pytest
import pickle
import os
from lazydag.collections import ListObjectCollection, DictObjectCollection

def test_list_collection_ops(tmp_path):
    save_path = tmp_path / "list_data"
    col = ListObjectCollection("test_list", str(save_path))
    
    # Initial state
    assert len(col) == 0
    
    # Insert
    col.insert(0, 10)
    col.insert(1, 20)
    col.insert(0, 5) # [5, 10, 20]
    
    assert len(col) == 3
    assert col.get(0) == 5
    assert col.get(1) == 10
    assert col.get(2) == 20
    
    # Remove
    col.remove(1) # [5, 20]
    assert len(col) == 2
    assert col.get(1) == 20
    
    # Set
    col.set(1, 99) # [5, 99]
    assert col.get(1) == 99

def test_list_collection_push(tmp_path):
    save_path = tmp_path / "list_push"
    col = ListObjectCollection("test_list", str(save_path))
    
    col.push(10)
    col.push(20)
    
    assert len(col) == 2
    assert col.get(0) == 10
    assert col.get(1) == 20
    
    # Check changelog recorded inserts
    assert len(col._changelog) == 2
    assert col._changelog[0] == ('insert', 0, 10)
    assert col._changelog[1] == ('insert', 1, 20)


def test_list_collection_views(tmp_path):
    save_path = tmp_path / "list_views"
    col = ListObjectCollection("test_list", str(save_path))
    
    # Save initial state
    col.insert(0, 100)
    col.save()
    
    # Modify
    col.insert(1, 200)
    col.set(0, 101)
    
    # Current view [101, 200]
    assert col.get(0, old=False) == 101
    assert col.get(1, old=False) == 200
    
    # Old view [100]
    # Note: ListCollection indices might shift. 
    # get(idx, old=True) accesses _data[idx]
    # _data has [100].
    assert col.get(0, old=True) == 100
    
    # Old view len? The collection len() method returns _current len.
    # But we can check internal _data
    assert len(col._data) == 1

def test_list_collection_changelog(tmp_path):
    save_path = tmp_path / "list_log"
    col = ListObjectCollection("test_list", str(save_path))
    
    col.insert(0, 1)
    col.insert(1, 2)
    col.set(0, 3)
    col.remove(1)
    
    # Changelog should have 4 entries
    assert len(col._changelog) == 4
    assert col._changelog[0] == ('insert', 0, 1)
    
    # Save clears log
    col.save()
    assert len(col._changelog) == 0
    assert col.get(0) == 3

def test_map_collection_ops(tmp_path):
    save_path = tmp_path / "map_data"
    col = DictObjectCollection("test_map", str(save_path))
    
    col.set("k1", "v1")
    col.set("k2", "v2")
    
    assert col.get("k1") == "v1"
    assert len(col) == 2
    
    col.remove("k1")
    assert col.get("k1") is None
    
    # Views
    col.save() # Persist {"k2": "v2"}
    
    col.set("k2", "v2_new")
    col.set("k3", "v3")
    
    assert col.get("k2", old=True) == "v2"
    assert col.get("k2", old=False) == "v2_new"
    
    assert col.get("k3", old=True) is None
    assert col.get("k3", old=False) == "v3"

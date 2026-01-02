import pytest
import json
from pathlib import Path
from lazydag.contrib.objects import FSJsonDictObject

def test_fs_json_dict_basic(tmp_path):
    # Note: FSJsonDictObject expects save_path to be a Path or str
    obj = FSJsonDictObject("test_dict", save_path=tmp_path)
    obj.on_add_to_pipeline()
    obj.on_pipeline_start()
    
    # Test set/get
    obj.set("key1", {"a": 1})
    assert obj.get("key1") == {"a": 1}
    assert obj.changed() is True
    
    # Test save
    obj.save()
    assert obj.changed() is False
    assert (tmp_path / "key1").exists()
    with open(tmp_path / "key1", "r") as f:
        assert json.load(f) == {"a": 1}

def test_fs_json_dict_persistence(tmp_path):
    # Setup initial data
    with open(tmp_path / "key1", "w") as f:
        json.dump("value1", f)
        
    obj = FSJsonDictObject("test_dict", save_path=tmp_path)
    obj.on_add_to_pipeline() # Ensures dir exists
    obj.on_pipeline_start()
    
    # Load from disk (underlay)
    assert obj.get("key1") == "value1"
    
    # Modify (overlay)
    obj.set("key1", "value1_new")
    assert obj.get("key1") == "value1_new"
    
    # Test old view
    assert obj.get("key1", old=True) == "value1"
    
    # Keys enumeration
    assert set(obj.keys()) == {"key1"}
    
    # Add new key
    obj.set("key2", 42)
    assert set(obj.keys()) == {"key1", "key2"}
    
    # Save commits changes
    obj.save()
    with open(tmp_path / "key1", "r") as f:
        assert json.load(f) == "value1_new"
    with open(tmp_path / "key2", "r") as f:
        assert json.load(f) == 42

def test_fs_json_dict_remove(tmp_path):
    # Setup disk data
    with open(tmp_path / "key1", "w") as f:
        json.dump("v1", f)
    with open(tmp_path / "key2", "w") as f:
        json.dump("v2", f)
        
    obj = FSJsonDictObject("test_dict", save_path=tmp_path)
    obj.on_add_to_pipeline()
    obj.on_pipeline_start()
    
    # Remove existing key
    obj.remove("key1")
    with pytest.raises(KeyError):
        obj.get("key1")
        
    # Still visible in old view
    assert obj.get("key1", old=True) == "v1"
    
    # Keys should not include removed key
    assert set(obj.keys()) == {"key2"}
    
    # Save should delete from disk
    obj.save()
    assert not (tmp_path / "key1").exists()
    assert (tmp_path / "key2").exists()

def test_fs_json_dict_validation(tmp_path):
    obj = FSJsonDictObject("test_dict", save_path=tmp_path)
    obj.on_add_to_pipeline()
    obj.on_pipeline_start()
    
    # Invalid key type
    with pytest.raises(ValueError, match="Key must be a string"):
        obj.set(123, "val")
        
    # Invalid key characters (only alphanumeric and underscore allowed)
    bad_keys = ["key-1", "key 1", "key.json", "", "key!"]
    for k in bad_keys:
        with pytest.raises(ValueError, match="Key must contain only English letters, numbers, and underscores"):
            obj.set(k, "val")
            
    # Valid key
    obj.set("Valid_Key_1", "ok")
    assert obj.get("Valid_Key_1") == "ok"

def test_fs_json_dict_cache_behavior(tmp_path):
    with open(tmp_path / "cached_key", "w") as f:
        json.dump("initial", f)
        
    obj = FSJsonDictObject("test_dict", save_path=tmp_path)
    obj.on_add_to_pipeline()
    obj.on_pipeline_start()
    
    # First get loads from disk into underlay
    assert obj.get("cached_key") == "initial"
    
    # Directly modify file on disk (simulating external change which shouldn't happen but tests underlay cache)
    with open(tmp_path / "cached_key", "w") as f:
        json.dump("modified_on_disk", f)
        
    # Should still return initial because of underlay cache
    assert obj.get("cached_key") == "initial"
    
    # old=True also uses underlay if it's there
    assert obj.get("cached_key", old=True) == "initial"
    
    # Save clears cache
    obj.save()
    assert obj.get("cached_key") == "modified_on_disk"

def test_fs_json_dict_non_existent(tmp_path):
    obj = FSJsonDictObject("test_dict", save_path=tmp_path)
    obj.on_add_to_pipeline()
    obj.on_pipeline_start()
    
    with pytest.raises(KeyError):
        obj.get("missing")

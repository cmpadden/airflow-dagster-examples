from joblib import Memory

cache_directory = ".cache"
memory = Memory(cache_directory)
persistent_cache = memory.cache

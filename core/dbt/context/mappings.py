from collections.abc import Mapping, MutableMapping
from collections import ChainMap


class LazyTransformedDict(MutableMapping):
    def __init__(self, dictionary, transform):
        self.dictionary = dictionary
        self.transform = transform
        self.transformed = {}

    def __getitem__(self, key):
        if key in self.transformed:
            return self.transformed[key]
        result = self.transform(self.dictionary[key])
        self.transformed[key] = result
        return result
    
    def __setitem__(self, key, value):
        raise NotImplementedError
    
    def __delitem__(self, key):
        raise NotImplementedError
    
    def __iter__(self):
        raise NotImplementedError
    
    def __len__(self):
        return len(self.dictionary)
    
    def __contains__(self, key):
        return key in self.dictionary
    
    def __bool__(self):
        return bool(self.dictionary)
    
    def items(self):
        raise NotImplementedError
    
    def keys(self):
        raise NotImplementedError
    
    def values(self):
        raise NotImplementedError


class MutableMappingWrapper(MutableMapping):
    def __init__(self, mapping):
        self.mapping = mapping

    def __getitem__(self, key):
        # if key == "dbt_unit_testing":
        #     import pdb; pdb.set_trace()
        #     print("Someone is trying to access dbt_unit_testing!!")
        result = self.mapping[key]
        # If result is a mapping, we wrap it in a new MutableMappingWrapper
        # if isinstance(result, Mapping) and not isinstance(result, MutableMappingWrapper):
        #     return MutableMappingWrapper(result)
        return result
    
    def __setitem__(self, key, value):
        self.mapping[key] = value

    def __delitem__(self, key):
        raise NotImplementedError
    
    def clear(self):
        raise NotImplementedError

    def __iter__(self):
        return iter({})
        # import pdb; pdb.set_trace()
        # print("We want to avoid expensive iteration of the mapping")
        # raise NotImplementedError

    def __len__(self):
        import pdb; pdb.set_trace()
        print("We want to avoid expensive iteration of the mapping")
        raise NotImplementedError
    
    def copy(self):
        # Instead of copying, we add a new map
        return MutableMappingWrapper(ChainMap({}, self.mapping))
    
    def update(self, other):
        # Instead of updating the mapping, we add a new map
        self.mapping = ChainMap({}, other, self.mapping)

    def __contains__(self, key):
        # if key == "dbt_unit_testing":
        #     import pdb; pdb.set_trace()
        #     print("Someone is trying to access dbt_unit_testing!!")
        return key in self.mapping
    
    def __bool__(self):
        return bool(self.mapping)
    
    def items(self):
        raise NotImplementedError
    
    def keys(self):
        return {}.keys()
    
    def values(self):
        raise NotImplementedError
    
class ImmutableMappingWrapper(Mapping):
    def __init__(self, mapping):
        self.mapping = mapping

    def __getitem__(self, key):
        # if key == "dbt_unit_testing":
        #     import pdb; pdb.set_trace()
        #     print("Someone is trying to access dbt_unit_testing!!")
        result = self.mapping[key]
        # If result is a mapping, we wrap it in a new ImmutableMappingWrapper
        # if isinstance(result, Mapping) and not isinstance(result, ImmutableMappingWrapper):
        #     return ImmutableMappingWrapper(result)
        return result
    
    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError
    
    def clear(self):
        raise NotImplementedError

    def __iter__(self):
        import pdb; pdb.set_trace()
        print("We want to avoid expensive iteration of the mapping")
        raise NotImplementedError

    def __len__(self):
        import pdb; pdb.set_trace()
        print("We want to avoid expensive iteration of the mapping")
        raise NotImplementedError
    
    def copy(self):
        return self
    
    def update(self, other):
        raise NotImplementedError

    def __contains__(self, key):
        # if key == "dbt_unit_testing":
        #     import pdb; pdb.set_trace()
        #     print("Someone is trying to access dbt_unit_testing!!")
        return key in self.mapping
    
    def __bool__(self):
        return bool(self.mapping)
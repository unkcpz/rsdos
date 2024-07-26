import typing as t
from pathlib import Path
from .rsdos import _Container

class Container:

    def __init__(
        self,
        folder: t.Union[str, Path]
    ):
        self.cnt = _Container(folder)

    def get_folder(self) -> Path:
        return self.cnt.get_folder()

    def get_object_content(self, hashkey: str) -> bytes:
        return bytes(self.cnt.get_object_content(hashkey))

    # XXX: althrough it is faster (2x faster) than legacy dos, but this is way more slower than 
    # the speed gained from `get_object_content` which is x30 faster.
    # legacy dos directly deal with the stream. If change it to using `get_object_content` it suffers from
    # the same overhead. Need to clear about where the overhead comes from.
    def get_objects_content(        
        self, hashkeys: t.List[str], skip_if_missing: bool = True
    ) -> t.Dict[str, t.Optional[bytes]]:
        retrieved = {}
        for hashkey in hashkeys:
            try:
                content = self.get_object_content(hashkey)
            except ValueError:
                if skip_if_missing:
                    continue
                else:
                    content = None

            retrieved[hashkey] = content

        return retrieved
        
        # direct rs wrapper
        # return {k: bytes(v) for k, v in self.cnt.get_objects_content(hashkeys).items()}

    def add_object(self, content: bytes) -> str:
        return self.cnt.add_object(content)

    def init_container(
        self,
        clear: bool = False,
        pack_size_target: int = 4 * 1024 * 1024 * 1024,
        loose_prefix_len: int = 2,
        hash_type: str = "sha256",
        compression_algorithm: str = "zlib+1",
    ) -> None:
        self.cnt.init_container(pack_size_target)

    @property
    def is_initialised(self) -> bool:
        return self.cnt.is_initialised

    def list_all_objects(self) -> t.Iterator[str]:
        """For loose it simply traverse the filename in loose store, so never will be the bottleneck
        I'll just use the python implementation. Using PyO3 to return iterator is complex."""
        for first_level in Path(self.get_folder() / "loose").iterdir():
            # if not self._is_valid_loose_prefix(first_level):
            #     continue
            for second_level in Path(self.get_folder() / "loose" / first_level).iterdir():
                hashkey = f"{first_level}{second_level}"
                # if not self._is_valid_hashkey(hashkey):
                #     continue
                yield hashkey



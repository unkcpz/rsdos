import typing as t
import io
from pathlib import Path
from .rsdos import _Container

# StreamReadBytesType = t.Union[
#     t.BinaryIO,
#     "PackedObjectReader",
#     "CallbackStreamWrapper",
#     "ZlibLikeBaseStreamDecompresser",
#     "ZeroStream",
# ]
StreamBytesType = t.BinaryIO
StreamReadBytesType = t.BinaryIO
StreamSeekBytesType = t.BinaryIO

class Container:

    def __init__(
        self,
        folder: t.Union[str, Path]
    ):
        self.cnt = _Container(folder)

    def get_folder(self) -> Path:
        return self.cnt.get_folder()

    def _fetch_from_loose(self, hashkey: str, stream: StreamBytesType):
        self.cnt.write_stream_from_loose(hashkey, stream)

    def iter_objects_stream_loose(
        self, hashkeys: t.List[str], skip_if_missing: bool = True
    ) -> t.Iterator[t.Tuple[str, t.Optional[StreamReadBytesType]]]:
        for hashkey in hashkeys:
            stream = io.BytesIO()
            try:
                self._fetch_from_loose(hashkey, stream)
                yield (hashkey, stream)
            except ValueError as exc:
                if skip_if_missing:
                    yield (hashkey, None)
                else:
                    raise exc from None

    def _fetch_from_packs(self, hashkey: str, stream: StreamBytesType):
        self.cnt.write_stream_from_packs(hashkey, stream)

    def iter_objects_stream_packs(
        self, hashkeys: t.List[str], skip_if_missing: bool = True
    ) -> t.Iterator[t.Tuple[str, t.Optional[StreamReadBytesType]]]:
        for hashkey in hashkeys:
            stream = io.BytesIO()
            try:
                self._fetch_from_packs(hashkey, stream)
                yield (hashkey, stream)
            except ValueError as exc:
                if skip_if_missing:
                    yield (hashkey, None)
                else:
                    raise exc from None


    def get_object_content(self, hashkey: str) -> bytes | None:
        stream = io.BytesIO()
        try:
            # try fetch from loose
            self._fetch_from_loose(hashkey, stream)
        except ValueError:
            try:
                # not found in loose, try fetch from packs
                self._fetch_from_packs(hashkey, stream)
            except ValueError:
                return None
            else:
                return stream.read()
        else:
            return stream.read()

    def get_objects_content(        
        self, hashkeys: t.List[str], skip_if_missing: bool = True
    ) -> t.Dict[str, t.Optional[bytes]]:
        d = self.get_loose_objects_content_raw_rs(hashkeys, skip_if_missing)

        # what not found in loose, try to find in packs
        # packs XXX: large speed overhead even no object in packs
        for k, v in self.cnt.stream_from_packs_multi(hashkeys).items():
            d[k] = bytes(v)

        return d
        
    def get_loose_objects_content_raw_rs(
        self, hashkeys: t.List[str], skip_if_missing: bool = True
    ) -> t.Dict[str, t.Optional[bytes]]:
        d = {}
        for k, v in self.cnt.get_loose_objects_content(hashkeys).items():
            if v is not None:
                d[k] = bytes(v)
            else:
                if skip_if_missing:
                    continue
                else:
                    d[k] = None

        return d

    def add_object(self, content: bytes) -> str:
        stream = io.BytesIO(content)
        return self.add_streamed_object(stream)

    def add_object_to_packs(self, content: bytes) -> str:
        stream = io.BytesIO(content)

        # import ipdb; ipdb.set_trace() 
        h = self.add_streamed_object_to_packs(stream)
        return h

    # XXX: I prefer name `add_objects_to_packs`
    def add_objects_to_pack(  # pylint: disable=too-many-arguments
        self,
        content_list: t.Union[t.List[bytes], t.Tuple[bytes, ...]],
        compress: bool = False,
        no_holes: bool = False,
        no_holes_read_twice: bool = True,
        callback: t.Optional[t.Callable] = None,
        do_fsync: bool = True,
        do_commit: bool = True,
    ) -> t.List[str]:
        stream_list: t.List[StreamSeekBytesType] = [
            io.BytesIO(content) for content in content_list
        ]
        hkey_lst = self.cnt.stream_to_packs_multi(stream_list)
        return hkey_lst
            

    def add_streamed_object(self, stream: StreamReadBytesType) -> str:
        _, hashkey = self.cnt.stream_to_loose(stream)

        return hashkey

    def add_streamed_object_to_packs(self, stream: StreamReadBytesType) -> str:
        _, hashkey = self.cnt.stream_to_packs(stream)

        return hashkey

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

    def get_total_size(self) -> int:
        return self.cnt.get_total_size()

    def count_objects(self) -> int:
        return self.cnt.get_n_objs()


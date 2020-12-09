import datetime
import logging
import urllib
import urlpath

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile


logger = logging.getLogger(__name__)


DCACHE_FILE_TYPES = {
    'REGULAR': 'file',
    'DIR': 'directory'
}


def _get_details(path, data):
    """

    :param path: (string)
    :param data: (dict)
    """
    path = urlpath.URL(path)

    name = data.get('fileName')
    name = path / name if name is not None else path
    name = name.path
    type = data.get('fileType')
    type = DCACHE_FILE_TYPES.get(type, 'other')
    created = data.get('creationTime')  # in ms
    created = datetime.datetime.fromtimestamp(created / 1000.)
    modified = data.get('mtime')  # in ms
    modified = datetime.datetime.fromtimestamp(modified / 1000.)
    return dict(
        name=name,
        size=data.get('size'),
        type=type,
        created=created,
        modified=modified
    )


def _encode(path):
    return urllib.parse.quote(path, safe='')


class dCacheFileSystem(AbstractFileSystem):

    def __init__(
        self,
        api_url=None,
        username=None,
        password=None,
        token=None,
        webdav_url=None,
        **storage_options
    ):
        super().__init__(self, **storage_options)

        self._api_url = api_url

        if (username is None) ^ (password is None):
            raise ValueError('Username or password not provided')
        self.username = username
        self.password = password

        if password is not None and token is not None:
            raise ValueError('Provide either token or username/password')
        self.token = token

        self._webdav_url = webdav_url

    @property
    def api_url(self):
        if self._api_url is None:
            raise ValueError("dCache API URL not provided.")
        return urlpath.URL(self._api_url)

    @property
    def webdav_url(self):
        if self._webdav_url is None:
            raise ValueError("WebDAV URL not provided.")
        return urlpath.URL(self._webdav_url)

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        return urlpath.URL(path).path

    @staticmethod
    def _get_kwargs_from_urls(path):
        kwargs = {}
        drive = urlpath.URL(path).drive
        if drive:
            kwargs.update(webdav_url=drive)
        return kwargs

    def kw(self, kwargs=None):
        kw = kwargs or {}
        if self.token is not None:
            headers = kw.get('headers', {})
            headers.update(Authorization=f'Bearer {self.token}')
            kw.update(headers=headers)
        if self.username is not None and self.password is not None:
            auth = (self.username, self.password)
            kw.update(auth=auth)
        return kw

    def _get_metadata(self, path, children=False, limit=None, **kwargs):
        path = self._strip_protocol(path)
        url = self.api_url / 'namespace' / _encode(path)
        url = url.with_query(children=children)
        if limit is not None:
            url = url.add_query(limit=f'{limit}')

        r = url.get(**self.kw(kwargs))
        r.raise_for_status()
        return r.json()

    def ls(self, path, detail=True, limit=None, **kwargs):
        path = self._strip_protocol(path)
        metadata = self._get_metadata(path, children=True, limit=limit,
                                      **kwargs)
        elements = metadata.get('children') or [metadata]
        details = [_get_details(path, el) for el in elements]
        if detail:
            return details
        else:
            return [d.get('name') for d in details]

    def mv(self, path1, path2, **kwargs):

        path1 = self._strip_protocol(path1)
        url = self.api_url / 'namespace' / _encode(path1)

        path2 = self._strip_protocol(path2)
        data = dict(action='mv', destination=path2)

        r = url.post(json=data, **self.kw(kwargs))
        r.raise_for_status()

    def _rm(self, path):

        path = self._strip_protocol(path)
        url = self.api_url / 'namespace' / _encode(path)

        r = url.delete(**self.kw())
        r.raise_for_status()

    def info(self, path, **kwargs):

        path = self._strip_protocol(path)
        metadata = self._get_metadata(path, **kwargs)
        return _get_details(path, metadata)

    def created(self, path):
        return self.info(path).get('created')

    def modified(self, path):
        return self.info(path).get('modified')

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return dCacheFile(
            fs=self,
            url=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs
        )

    def open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        **kwargs
    ):
        options = self._get_kwargs_from_urls(path)
        self._webdav_url = options.get('webdav_url', self._webdav_url)
        return super().open(
            path=path,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs
        )


class dCacheFile(AbstractBufferedFile):
    def __init__(self, fs, url, **kwargs):
        path = fs._strip_protocol(url)
        super().__init__(fs=fs, path=path, **kwargs)

    def _fetch_range(self, start, end):
        url = self.fs.webdav_url / self.path

        kwargs = self.kwargs.copy()
        headers = kwargs.pop('headers', {})
        headers.update(Range="bytes=%i-%i" % (start, end - 1))
        kwargs.update(headers=headers)
        kwargs = self.fs.kw(kwargs)

        r = url.get(**kwargs)

        if r.status_code == 416:
            # range request outside file
            return b""
        r.raise_for_status()
        if r.status_code == 206:
            # partial content, as expected
            out = r.content
        elif "Content-Length" in r.headers:
            cl = int(r.headers["Content-Length"])
            if cl <= end - start:
                # data size OK
                out = r.content
            else:
                raise ValueError(
                    f"Got more bytes ({cl}) than requested ({end - start})"
                )
        else:
            cl = 0
            out = []
            for chunk in r.iter_content(chunk_size=2 ** 20):
                # data size unknown, let's see if it goes too big
                if chunk:
                    out.append(chunk)
                    cl += len(chunk)
                    if cl > end - start:
                        raise ValueError(f"Got more bytes so far (>{cl})"
                                         f" than requested ({end - start})")
            out = b"".join(out)
        return out

    def _upload_chunk(self, final=False):
        url = self.fs.webdav_url / self.path

        data = self.buffer.getvalue()
        start = self.offset
        end = len(data)

        kwargs = self.kwargs.copy()
        headers = kwargs.pop('headers', {})
        headers.update(Range="bytes=%i-%i" % (start, end - 1))
        kwargs.update(headers=headers)
        kwargs = self.fs.kw(kwargs)

        r = url.put(data=data, **kwargs)
        r.raise_for_status()
        return True
